#ifndef DISCORD_NOTIFY_H_
#define DISCORD_NOTIFY_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "global_state.h"

/**
 * Discord webhook notifications.
 *
 * Events are never sent from the caller's context. They are stored in a small
 * per-event slot and delivered by a dedicated task, so calling any of the
 * discord_notify_* functions is cheap and never blocks mining or the stratum
 * tasks. Rapidly repeating events are coalesced (see min intervals in the .c).
 */

// Create the delivery task. Call once after the network stack is up.
void discord_notify_init(GlobalState *gs);

// True if `url` looks like a Discord webhook URL (https, discord host, /api/webhooks/...).
bool discord_notify_webhook_url_valid(const char *url);

// A share reached the network difficulty.
void discord_notify_block_found(double diff, double network_diff, int blocks_found);

// A share difficulty was recorded. Chooses between the "best all-time" and the
// "new session high" message depending on which records were broken and which
// notifications are enabled.
void discord_notify_share_diff(uint64_t diff, uint64_t previous_all_time_best,
                               bool new_session_best, bool new_all_time_best);

// Failover to the fallback pool (using_fallback = true) or back to the primary.
void discord_notify_pool_switch(bool using_fallback, const char *pool_url, uint16_t pool_port, bool manual);

// Queue a test message. Returns false when no webhook URL is configured.
bool discord_notify_send_test(void);

// Short status of the last delivery attempt ("never", "ok", "http 404", ...).
void discord_notify_get_status(char *buf, size_t len);

#endif // DISCORD_NOTIFY_H_
