#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <ctype.h>

#include "cJSON.h"
#include "esp_crt_bundle.h"
#include "esp_http_client.h"
#include "esp_log.h"
#include "esp_timer.h"
#include "esp_heap_caps.h"
#include "esp_system.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/task.h"

#include "discord_notify.h"
#include "nvs_config.h"
#include "utils.h"

static const char *TAG = "discord";

typedef enum {
    EV_BLOCK_FOUND = 0,   // highest priority first
    EV_FALLBACK,
    EV_BEST_DIFF,
    EV_NEW_HIGH_DIFF,
    EV_TEST,
    EV_COUNT
} discord_event_t;

// Minimum time between two deliveries of the same event type. A newer event of
// the same type replaces a still-pending older one, so the latest value wins.
static const int64_t MIN_INTERVAL_US[EV_COUNT] = {
    [EV_BLOCK_FOUND]   = 0,
    [EV_FALLBACK]      = 5LL * 1000 * 1000,
    [EV_BEST_DIFF]     = 60LL * 1000 * 1000,
    [EV_NEW_HIGH_DIFF] = 300LL * 1000 * 1000,
    [EV_TEST]          = 0,
};

#define MAX_FIELDS      5
#define MAX_ATTEMPTS    4
#define RETRY_DELAY_US  (15LL * 1000 * 1000)

typedef struct {
    char name[28];
    char value[112];
} embed_field_t;

typedef struct {
    bool valid;
    uint8_t attempts;
    int64_t not_before_us;
    uint32_t color;
    char title[80];
    char description[200];
    embed_field_t fields[MAX_FIELDS];
    uint8_t field_count;
} slot_t;

static GlobalState *s_gs = NULL;
static slot_t s_slots[EV_COUNT];
static int64_t s_last_sent_us[EV_COUNT];
static SemaphoreHandle_t s_mutex = NULL;
static TaskHandle_t s_task = NULL;
static char s_status[64] = "never";

// ---------------------------------------------------------------- helpers

bool discord_notify_webhook_url_valid(const char *url)
{
    static const char *const prefixes[] = {
        "https://discord.com/api/webhooks/",
        "https://discordapp.com/api/webhooks/",
        "https://ptb.discord.com/api/webhooks/",
        "https://canary.discord.com/api/webhooks/",
    };
    if (url == NULL) return false;
    size_t len = strlen(url);
    if (len > 255) return false;

    const char *rest = NULL;
    for (size_t i = 0; i < sizeof(prefixes) / sizeof(prefixes[0]); i++) {
        size_t plen = strlen(prefixes[i]);
        if (strncmp(url, prefixes[i], plen) == 0) { rest = url + plen; break; }
    }
    if (rest == NULL || *rest == '\0') return false;

    for (const char *c = rest; *c; c++) {
        if (!(isalnum((unsigned char)*c) || *c == '/' || *c == '-' || *c == '_' ||
              *c == '?' || *c == '=' || *c == '&' || *c == '.')) {
            return false;
        }
    }
    return true;
}

static void set_status(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    char tmp[sizeof(s_status)];
    vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    if (s_mutex && xSemaphoreTake(s_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        strlcpy(s_status, tmp, sizeof(s_status));
        xSemaphoreGive(s_mutex);
    }
}

void discord_notify_get_status(char *buf, size_t len)
{
    if (!buf || len == 0) return;
    buf[0] = '\0';
    if (s_mutex && xSemaphoreTake(s_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        strlcpy(buf, s_status, len);
        xSemaphoreGive(s_mutex);
    } else {
        strlcpy(buf, "unknown", len);
    }
}

// Returns a malloc'ed copy of the webhook URL when it is set and valid.
static char *get_webhook_url(void)
{
    char *url = nvs_config_get_string(NVS_CONFIG_DISCORD_WEBHOOK_URL);
    if (url == NULL) return NULL;
    if (url[0] == '\0' || !discord_notify_webhook_url_valid(url)) {
        free(url);
        return NULL;
    }
    return url;
}

static void fmt_diff(double value, char *buf, size_t len)
{
    if (value < 0) value = 0;
    suffixString((uint64_t)value, buf, len, 2);
}

static void fmt_hashrate(char *buf, size_t len)
{
    float ghs = s_gs ? s_gs->SYSTEM_MODULE.current_hashrate : 0;
    if (ghs >= 1000.0f) {
        snprintf(buf, len, "%.2f TH/s", ghs / 1000.0f);
    } else {
        snprintf(buf, len, "%.1f GH/s", ghs);
    }
}

static void add_field(slot_t *s, const char *name, const char *value)
{
    if (s->field_count >= MAX_FIELDS) return;
    strlcpy(s->fields[s->field_count].name, name, sizeof(s->fields[0].name));
    strlcpy(s->fields[s->field_count].value, value, sizeof(s->fields[0].value));
    s->field_count++;
}

static void add_hashrate_field(slot_t *s)
{
    char hr[48];
    fmt_hashrate(hr, sizeof(hr));
    add_field(s, "Hashrate", hr);
}

// Store an event in its slot (replacing a pending one of the same type) and wake the task.
static void post_event(discord_event_t ev, const slot_t *msg)
{
    if (s_mutex == NULL || s_task == NULL) return;
    if (xSemaphoreTake(s_mutex, pdMS_TO_TICKS(200)) != pdTRUE) return;
    s_slots[ev] = *msg;
    s_slots[ev].valid = true;
    s_slots[ev].attempts = 0;
    s_slots[ev].not_before_us = 0;
    xSemaphoreGive(s_mutex);
    xTaskNotifyGive(s_task);
}

static void begin_msg(slot_t *s, uint32_t color, const char *title)
{
    memset(s, 0, sizeof(*s));
    s->color = color;
    strlcpy(s->title, title, sizeof(s->title));
}

// -------------------------------------------------------------- public API

void discord_notify_block_found(double diff, double network_diff, int blocks_found)
{
    if (!nvs_config_get_bool(NVS_CONFIG_DISCORD_NOTIFY_BLOCK)) return;
    char *url = get_webhook_url();
    if (!url) return;
    free(url);

    slot_t m;
    char a[24], b[24], n[16];
    begin_msg(&m, 0x2ECC71, "\xF0\x9F\x8E\x89 BLOCK FOUND!");
    strlcpy(m.description, "A share met the network difficulty. Check your pool for the block.", sizeof(m.description));
    fmt_diff(diff, a, sizeof(a));
    fmt_diff(network_diff, b, sizeof(b));
    snprintf(n, sizeof(n), "%d", blocks_found);
    add_field(&m, "Share difficulty", a);
    add_field(&m, "Network difficulty", b);
    add_field(&m, "Blocks found (since boot)", n);
    add_hashrate_field(&m);
    post_event(EV_BLOCK_FOUND, &m);
}

void discord_notify_share_diff(uint64_t diff, uint64_t previous_all_time_best,
                               bool new_session_best, bool new_all_time_best)
{
    bool want_best = new_all_time_best && nvs_config_get_bool(NVS_CONFIG_DISCORD_NOTIFY_BEST_DIFF);
    bool want_high = new_session_best && nvs_config_get_bool(NVS_CONFIG_DISCORD_NOTIFY_NEW_HIGH_DIFF);
    if (!want_best && !want_high) return;

    char *url = get_webhook_url();
    if (!url) return;
    free(url);

    slot_t m;
    char cur[24], prev[24], all[24];
    fmt_diff((double)diff, cur, sizeof(cur));

    if (want_best) {
        begin_msg(&m, 0xF1C40F, "\xF0\x9F\x8F\x86 New all-time best difficulty");
        strlcpy(m.description, "This miner just beat its own all-time record.", sizeof(m.description));
        fmt_diff((double)previous_all_time_best, prev, sizeof(prev));
        add_field(&m, "New best", cur);
        add_field(&m, "Previous best", prev);
        add_hashrate_field(&m);
        post_event(EV_BEST_DIFF, &m);
        return;
    }

    // new session high only
    begin_msg(&m, 0x3498DB, "\xE2\xAC\x86\xEF\xB8\x8F New session high difficulty");
    strlcpy(m.description, "Best share since this miner booted.", sizeof(m.description));
    fmt_diff((double)previous_all_time_best, all, sizeof(all));
    add_field(&m, "Session best", cur);
    add_field(&m, "All-time best", all);
    add_hashrate_field(&m);
    post_event(EV_NEW_HIGH_DIFF, &m);
}

void discord_notify_pool_switch(bool using_fallback, const char *pool_url, uint16_t pool_port, bool manual)
{
    if (!nvs_config_get_bool(NVS_CONFIG_DISCORD_NOTIFY_FALLBACK)) return;
    char *url = get_webhook_url();
    if (!url) return;
    free(url);

    slot_t m;
    char pool[96];
    snprintf(pool, sizeof(pool), "%s:%u", (pool_url && pool_url[0]) ? pool_url : "unknown", (unsigned)pool_port);

    if (using_fallback) {
        begin_msg(&m, 0xE67E22, "\xE2\x9A\xA0\xEF\xB8\x8F Switched to fallback pool");
        strlcpy(m.description,
                manual ? "The fallback pool was selected manually."
                       : "The primary pool is unreachable, mining continues on the fallback pool.",
                sizeof(m.description));
        add_field(&m, "Now mining on", pool);
    } else {
        begin_msg(&m, 0x2ECC71, "\xE2\x9C\x85 Primary pool restored");
        strlcpy(m.description, "Mining is back on the primary pool.", sizeof(m.description));
        add_field(&m, "Now mining on", pool);
    }
    add_hashrate_field(&m);
    post_event(EV_FALLBACK, &m);
}

bool discord_notify_send_test(void)
{
    char *url = get_webhook_url();
    if (!url) return false;
    free(url);

    slot_t m;
    begin_msg(&m, 0x95A5A6, "\xF0\x9F\x94\x94 Test notification");
    strlcpy(m.description, "Discord notifications are set up correctly.", sizeof(m.description));
    add_hashrate_field(&m);
    post_event(EV_TEST, &m);
    return true;
}

// ----------------------------------------------------------- delivery task

static char *build_payload(const slot_t *m)
{
    const char *host = (s_gs && s_gs->SYSTEM_MODULE.mdns_hostname[0]) ? s_gs->SYSTEM_MODULE.mdns_hostname : "miner";

    cJSON *root = cJSON_CreateObject();
    char username[96];
    snprintf(username, sizeof(username), "AxeOS %s", host);
    cJSON_AddStringToObject(root, "username", username);

    // Never let pool names or other text trigger @mentions.
    cJSON *allowed = cJSON_AddObjectToObject(root, "allowed_mentions");
    cJSON_AddItemToObject(allowed, "parse", cJSON_CreateArray());

    cJSON *embeds = cJSON_AddArrayToObject(root, "embeds");
    cJSON *e = cJSON_CreateObject();
    cJSON_AddStringToObject(e, "title", m->title);
    cJSON_AddStringToObject(e, "description", m->description);
    cJSON_AddNumberToObject(e, "color", m->color);

    cJSON *fields = cJSON_AddArrayToObject(e, "fields");
    for (int i = 0; i < m->field_count; i++) {
        cJSON *f = cJSON_CreateObject();
        cJSON_AddStringToObject(f, "name", m->fields[i].name);
        cJSON_AddStringToObject(f, "value", m->fields[i].value);
        cJSON_AddBoolToObject(f, "inline", true);
        cJSON_AddItemToArray(fields, f);
    }

    cJSON *footer = cJSON_AddObjectToObject(e, "footer");
    cJSON_AddStringToObject(footer, "text", host);

    time_t now = time(NULL);
    if (now > 1600000000) {  // only when the clock has been synced
        struct tm tmv;
        char ts[32];
        gmtime_r(&now, &tmv);
        strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%SZ", &tmv);
        cJSON_AddStringToObject(e, "timestamp", ts);
    }

    cJSON_AddItemToArray(embeds, e);
    char *out = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    return out;
}

// Returns true when delivered or when retrying cannot help.
static bool deliver(const slot_t *m, bool *retry)
{
    *retry = false;
    char *url = get_webhook_url();
    if (!url) {
        set_status("no webhook set");
        return true;
    }
    char *body = build_payload(m);
    if (!body) {
        free(url);
        set_status("out of memory");
        *retry = true;
        return false;
    }

    esp_http_client_config_t cfg = {
        .url = url,
        .method = HTTP_METHOD_POST,
        .timeout_ms = 20000,
        .crt_bundle_attach = esp_crt_bundle_attach,
        .buffer_size = 1024,
        .buffer_size_tx = 1024,
    };
    esp_http_client_handle_t client = esp_http_client_init(&cfg);
    bool ok = false;
    if (client) {
        esp_http_client_set_header(client, "Content-Type", "application/json");
        esp_http_client_set_post_field(client, body, (int)strlen(body));
        esp_err_t err = esp_http_client_perform(client);
        if (err == ESP_OK) {
            int status = esp_http_client_get_status_code(client);
            if (status >= 200 && status < 300) {
                ok = true;
                set_status("ok");
                ESP_LOGI(TAG, "Sent: %s", m->title);
            } else {
                // 429 and 5xx are worth retrying; 4xx (bad/deleted webhook) is not.
                *retry = (status == 429 || status >= 500);
                set_status("http %d", status);
                ESP_LOGW(TAG, "Discord returned HTTP %d", status);
            }
        } else {
            *retry = true;
            const char *name = esp_err_to_name(err);
            if (strncmp(name, "ESP_ERR_", 8) == 0) name += 8;   // shorter for the UI
            int sock_errno = esp_http_client_get_errno(client);
            int tls_code = 0, tls_flags = 0;
            esp_http_client_get_and_clear_last_tls_error(client, &tls_code, &tls_flags);
            // tls_flags holds the mbedTLS certificate verification flags (0x8 = issuer not trusted,
            // 0x1 = expired, 0x2 = name mismatch, ...); tls_code is the mbedTLS/esp-tls error.
            set_status("send failed: %s tls=%d flags=0x%x e%d", name, tls_code, (unsigned)tls_flags, sock_errno);
            ESP_LOGW(TAG, "Request failed: %s (0x%x), tls error %d (0x%x), cert flags 0x%x, socket errno %d, free heap %u / internal %u",
                     esp_err_to_name(err), (unsigned)err, tls_code, (unsigned)tls_code, (unsigned)tls_flags, sock_errno,
                     (unsigned)esp_get_free_heap_size(),
                     (unsigned)heap_caps_get_free_size(MALLOC_CAP_INTERNAL));
        }
        esp_http_client_cleanup(client);
    } else {
        *retry = true;
        set_status("client init failed");
    }
    free(body);
    free(url);
    return ok;
}

static void discord_task(void *arg)
{
    (void)arg;
    for (;;) {
        ulTaskNotifyTake(pdTRUE, pdMS_TO_TICKS(1000));

        if (!s_gs || !s_gs->SYSTEM_MODULE.is_connected) continue;

        const int64_t now = esp_timer_get_time();
        slot_t msg;
        memset(&msg, 0, sizeof(msg));
        int picked = -1;

        if (xSemaphoreTake(s_mutex, pdMS_TO_TICKS(200)) == pdTRUE) {
            for (int ev = 0; ev < EV_COUNT; ev++) {
                if (!s_slots[ev].valid) continue;
                if (now < s_slots[ev].not_before_us) continue;
                if (now - s_last_sent_us[ev] < MIN_INTERVAL_US[ev]) continue;
                msg = s_slots[ev];
                s_slots[ev].valid = false;
                s_last_sent_us[ev] = now;
                picked = ev;
                break;
            }
            xSemaphoreGive(s_mutex);
        }
        if (picked < 0) continue;

        bool retry = false;
        if (!deliver(&msg, &retry) && retry && msg.attempts + 1 < MAX_ATTEMPTS) {
            if (xSemaphoreTake(s_mutex, pdMS_TO_TICKS(200)) == pdTRUE) {
                // Do not overwrite a newer event that arrived in the meantime.
                if (!s_slots[picked].valid) {
                    s_slots[picked] = msg;
                    s_slots[picked].valid = true;
                    s_slots[picked].attempts = msg.attempts + 1;
                    s_slots[picked].not_before_us = esp_timer_get_time() + RETRY_DELAY_US;
                }
                xSemaphoreGive(s_mutex);
            }
        }

        // Stay within Discord's webhook rate limit (about 5 requests / 2 s).
        vTaskDelay(pdMS_TO_TICKS(1200));
        // Wake ourselves if something else is still pending.
        xTaskNotifyGive(xTaskGetCurrentTaskHandle());
    }
}

void discord_notify_init(GlobalState *gs)
{
    if (s_task != NULL) return;
    s_gs = gs;
    s_mutex = xSemaphoreCreateMutex();
    if (s_mutex == NULL) {
        ESP_LOGE(TAG, "Failed to create mutex");
        return;
    }
    memset(s_slots, 0, sizeof(s_slots));
    // Start the "quiet period" now so a fresh boot does not immediately report
    // the first (tiny) shares as new records.
    int64_t now = esp_timer_get_time();
    for (int i = 0; i < EV_COUNT; i++) s_last_sent_us[i] = (MIN_INTERVAL_US[i] > 0) ? now : INT64_MIN / 2;

    if (xTaskCreateWithCaps(discord_task, "discord", 12288, NULL, 3, &s_task, MALLOC_CAP_SPIRAM) != pdPASS) {
        ESP_LOGE(TAG, "Failed to create discord task");
        s_task = NULL;
    }
}
