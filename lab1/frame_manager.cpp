#include "include/storage_engine/buffer/frame_manager.h"

FrameManager::FrameManager(const char *tag) : allocator_(tag)
{}

RC FrameManager::init(int pool_num)
{
  int ret = allocator_.init(false, pool_num);
  if (ret == 0) {
    return RC::SUCCESS;
  }
  return RC::NOMEM;
}

RC FrameManager::cleanup()
{
  if (frames_.count() > 0) {
    return RC::INTERNAL;
  }
  frames_.destroy();
  return RC::SUCCESS;
}

Frame *FrameManager::alloc(int file_desc, PageNum page_num)
{
  FrameId frame_id(file_desc, page_num);
  std::lock_guard<std::mutex> lock_guard(lock_);
  Frame *frame = get_internal(frame_id);
  if (frame != nullptr) {
    return frame;
  }

  frame = allocator_.alloc();
  if (frame != nullptr) {
    ASSERT(frame->pin_count() == 0, "got an invalid frame that pin count is not 0. frame=%s",
        to_string(*frame).c_str());
    frame->set_page_num(page_num);
    frame->pin();
    frames_.put(frame_id, frame);
  }
  return frame;
}

Frame *FrameManager::get(int file_desc, PageNum page_num)
{
  FrameId frame_id(file_desc, page_num);
  std::lock_guard<std::mutex> lock_guard(lock_);
  return get_internal(frame_id);
}

bool pin_equals_notzero(Frame *frame)
{
  return frame->pin_count() != 0;
}



/**
 * TODO [Lab1] 需要同学们实现页帧驱逐
 */
int FrameManager::evict_frames(int count, std::function<RC(Frame *frame)> evict_action)
{
  int evicted_count = 0;

  // 从尾部开始遍历LRU链表
  Frame **frame = frames_.foreach_value(pin_equals_notzero);
  Frame *frame_temp = nullptr;
  while (frame != nullptr && evicted_count < count) {
    frame_temp = *frame;
    if (frame_temp->pin_count() == 0) {
      // 如果pin count为0，可以被驱逐
      if (evict_action(frame_temp) == RC::SUCCESS) {
        // 释放frame
        frames_.remove(frame_temp->frame_id());
        allocator_.free(frame_temp);
        evicted_count++;
      }
    }
        frame = frames_.foreach_value(pin_equals_notzero);

  }

  return evicted_count;
}

RC evict_action(Frame *frame)
{
    //将脏数据的frame刷回磁盘
        if (frame->dirty()) {
          //std::scoped_lock lock_guard(lock_);
          // 获取页面在文件中的偏移量
          int64_t offset = ((int64_t)frame->page_num()) * BP_PAGE_SIZE;
          // 计算该页面在文件中的偏移量，并将数据写入文件的目标位置
          if (lseek(frame->file_desc(), offset, SEEK_SET) == -1) {
            LOG_ERROR("Failed to flush page %d:%d, due to failed to lseek:%s.", frame->file_desc(), frame->page_num(), strerror(errno));
            return RC::IOERR_SEEK;
          }
          // 写入数据到文件的目标位置
          Page &page = frame->page();
          if (common::writen(frame->file_desc(), &page, BP_PAGE_SIZE) != 0) {
            LOG_ERROR("Failed to flush page , file_desc:%d, page num:%d, due to failed to write data:%s",
                      frame->file_desc(), frame->page_num(), strerror(errno));
            return RC::IOERR_WRITE;
          }
          // 清除frame的脏标记
          frame->clear_dirty();
          return RC::SUCCESS;
        }

    return RC::SUCCESS;
}

/**
 * @brief 从缓冲池中移除指定页面
 */
RC FrameManager::remove_frame(int file_desc, PageNum page_num)
{
  FrameId frame_id(file_desc, page_num);
  std::lock_guard<std::mutex> lock_guard(lock_);
  Frame *frame = get_internal(frame_id);
  if (frame != nullptr) {
    frames_.remove(frame_id);
    allocator_.free(frame);
    return RC::SUCCESS;
  }
  return RC::EMPTY;
}

Frame *FrameManager::get_internal(const FrameId &frame_id)
{
  Frame *frame = nullptr;
  (void)frames_.get(frame_id, frame);
  if (frame != nullptr) {
    frame->pin();
  }
  return frame;
}

/**
 * @brief 查找目标文件的frame
 * FramesCache中选出所有与给定文件描述符(file_desc)相匹配的Frame对象，并将它们添加到列表中
 */
std::list<Frame *> FrameManager::find_list(int file_desc)
{
  std::lock_guard<std::mutex> lock_guard(lock_);

  std::list<Frame *> frames;
  auto fetcher = [&frames, file_desc](const FrameId &frame_id, Frame *const frame) -> bool {
    if (file_desc == frame_id.file_desc()) {
      frame->pin();
      frames.push_back(frame);
    }
    return true;
  };
  frames_.foreach (fetcher);
  return frames;
}