const facebookApi = require('../facebook-api');
const { publishSendFailed } = require('../kafka-producer');
const { isProcessed, markProcessed, tryClaim, clearProcessed } = require('../idempotency-store');

/**
 * Xử lý một reply_command hoặc send_retry từ Kafka.
 * - Gọi Facebook Graph API tương ứng với action
 * - Nếu thành công: log success
 * - Nếu thất bại: publish send_failed
 */
const handleCommand = async (command) => {
  const { command_id, event_id, action, target, reply_text } = command;
  // retry_count có trong send_retry messages; mặc định 0 nếu lần đầu
  const retryCount = command.retry_count ?? 0;

  console.log(`\n[Command Handler] Xử lý command [${command_id}]`);
  console.log(`  action      : ${action}`);
  console.log(`  comment_id  : ${target?.comment_id || 'N/A'}`);
  console.log(`  reply_text  : ${reply_text || 'N/A'}`);
  console.log(`  retry_count : ${retryCount}`);

  // Idempotency: attempt to atomically claim this command_id before processing.
  // If claim fails, another consumer is processing or already processed it.
  try {
    const claimed = await tryClaim(command_id);
    if (!claimed) {
      console.log(`[Command Handler] ▶ SKIP - command already processed or claimed: ${command_id}`);
      return;
    }
  } catch (err) {
    console.error('[Command Handler] Idempotency claim failed:', err.message);
    // proceed (fail open) to avoid blocking processing
  }

  try {
    switch (action) {
      case 'reply': {
        if (!target?.comment_id) throw new Error('Thiếu comment_id để reply');
        await facebookApi.replyComment(target.comment_id, reply_text);
        console.log(`[Command Handler] ✅ SUCCESS - reply comment ${target.comment_id}`);
        break;
      }

      case 'hide': {
        if (!target?.comment_id) throw new Error('Thiếu comment_id để hide');
        await facebookApi.hideComment(target.comment_id);
        console.log(`[Command Handler] ✅ SUCCESS - hide comment ${target.comment_id}`);
        break;
      }

      case 'delete': {
        if (!target?.comment_id) throw new Error('Thiếu comment_id để delete');
        await facebookApi.deleteComment(target.comment_id);
        console.log(`[Command Handler] 🗑️ SUCCESS - delete comment ${target.comment_id}`);
        break;
      }

      case 'create_post': {
        const pageId = target?.page_id || 'me';
        await facebookApi.createPost(pageId, reply_text);
        console.log(`[Command Handler] ✅ SUCCESS - create post`);
        break;
      }

      default:
        console.warn(`[Command Handler]  Unknown action: "${action}" - bỏ qua`);
        return;
    }
    // Nếu reach tới đây tức là action thực thi thành công → (tùy chọn) refresh TTL
    try {
      await markProcessed(command_id);
    } catch (err) {
      console.error('[Command Handler] Failed to mark idempotency:', err.message);
    }
  } catch (error) {
    console.error(`[Command Handler] ❌ FAILED - ${error.message}`);

    // Publish tới send_failed với retry_count + 1
    await publishSendFailed(command, retryCount + 1, error.message);
    // Khi xử lý lỗi — xoá claim để cho phép retry consumer xử lý sau này
    try {
      await clearProcessed(command_id);
    } catch (err) {
      console.error('[Command Handler] Failed to clear idempotency after error:', err.message);
    }
  }
};

module.exports = { handleCommand };
