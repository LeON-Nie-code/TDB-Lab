#include "include/storage_engine/recover/log_manager.h"
#include "include/storage_engine/transaction/trx.h"

RC LogEntryIterator::init(LogFile &log_file)
{
  log_file_ = &log_file;
  return RC::SUCCESS;
}

RC LogEntryIterator::next()
{
  LogEntryHeader header;
  RC rc = log_file_->read(reinterpret_cast<char *>(&header), sizeof(header));
  if (rc != RC::SUCCESS) {
    if (log_file_->eof()) {
      return RC::RECORD_EOF;
    }
    LOG_WARN("failed to read log header. rc=%s", strrc(rc));
    return rc;
  }

  char *data = nullptr;
  int32_t entry_len = header.log_entry_len_;
  if (entry_len > 0) {
    data = new char[entry_len];
    rc = log_file_->read(data, entry_len);
    if (RC_FAIL(rc)) {
      LOG_WARN("failed to read log data. data size=%d, rc=%s", entry_len, strrc(rc));
      delete[] data;
      data = nullptr;
      return rc;
    }
  }

  if (log_entry_ != nullptr) {
    delete log_entry_;
    log_entry_ = nullptr;
  }
  log_entry_ = LogEntry::build(header, data);
  delete[] data;
  return rc;
}

bool LogEntryIterator::valid() const
{
  return log_entry_ != nullptr;
}

const LogEntry &LogEntryIterator::log_entry()
{
  return *log_entry_;
}

////////////////////////////////////////////////////////////////////////////////

LogManager::~LogManager()
{
  if (log_buffer_ != nullptr) {
    delete log_buffer_;
    log_buffer_ = nullptr;
  }

  if (log_file_ != nullptr) {
    delete log_file_;
    log_file_ = nullptr;
  }
}

RC LogManager::init(const char *path)
{
  log_buffer_ = new LogBuffer();
  log_file_   = new LogFile();
  return log_file_->init(path);
}

RC LogManager::append_begin_trx_log(int32_t trx_id)
{
  return append_log(LogEntry::build_mtr_entry(LogEntryType::MTR_BEGIN, trx_id));
}

RC LogManager::append_rollback_trx_log(int32_t trx_id)
{
  return append_log(LogEntry::build_mtr_entry(LogEntryType::MTR_ROLLBACK, trx_id));
}

RC LogManager::append_commit_trx_log(int32_t trx_id, int32_t commit_xid)
{
  RC rc = append_log(LogEntry::build_commit_entry(trx_id, commit_xid));
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to append trx commit log. trx id=%d, rc=%s", trx_id, strrc(rc));
    return rc;
  }
  LOG_INFO("commit trx=%d", trx_id);
  rc = sync(); // 事务提交时需要把当前事务关联的日志项都写入到磁盘中，这样做是保证不丢数据
  return rc;
}

RC LogManager::append_record_log(LogEntryType type, int32_t trx_id, int32_t table_id, const RID &rid, int32_t data_len, int32_t data_offset, const char *data)
{
  LogEntry *log_entry = LogEntry::build_record_entry(type, trx_id, table_id, rid, data_len, data_offset, data);
  if (nullptr == log_entry) {
    LOG_WARN("failed to create log entry");
    return RC::NOMEM;
  }
  return append_log(log_entry);
}

RC LogManager::append_log(LogEntry *log_entry)
{
  if (nullptr == log_entry) {
    return RC::INVALID_ARGUMENT;
  }
  return log_buffer_->append_log_entry(log_entry);
}

RC LogManager::sync()
{
  return log_buffer_->flush_buffer(*log_file_);
}

// TODO [Lab5] 需要同学们补充代码，相关提示见文档
RC LogManager::recover(Db *db)
{
  TrxManager *trx_manager = GCTX.trx_manager_;
  ASSERT(trx_manager != nullptr, "cannot do recover that trx_manager is null");

  // TODO [Lab5] 需要同学们补充代码，相关提示见文档

  LogEntryIterator log_entry_iter;
  RC rc = log_entry_iter.init(*log_file_);
  if (rc != RC::SUCCESS) {
  LOG_WARN("failed to init log entry iterator. rc=%s", strrc(rc));
  return rc;
  }

  int count = 0;
  // TODO 会不会跳过第一条日志？
  for (rc = log_entry_iter.next(); rc == RC::SUCCESS; rc = log_entry_iter.next()) {
    const LogEntry &log_entry = log_entry_iter.log_entry();
    LOG_INFO("recover log entry: %s", log_entry.to_string().c_str());
    LOG_INFO("count: %d", count++);
    switch (log_entry.log_type()) {
      case LogEntryType::MTR_BEGIN: {
      } break;

      case LogEntryType::MTR_COMMIT: {
        trx_manager->create_trx(log_entry.trx_id());//创建事务,怎么使用Trx *MvccTrxManager::create_trx(int32_t trx_id)

      } break;

      case LogEntryType::MTR_ROLLBACK: {
        trx_manager->create_trx(log_entry.trx_id());//创建事务,怎么使用Trx *MvccTrxManager::create_trx(int32_t trx_id)

      } break;

      case LogEntryType::INSERT: {
      } break;
      case LogEntryType::DELETE: {
      } break;
      case LogEntryType::ERROR: {
      } break;

      default: {
        LOG_WARN("unknown log type=%d", static_cast<int>(log_entry.log_type()));
      } break;
    }
  }

  LogEntryIterator new_log_entry_iter;
  log_file_->reset_read();
        RC new_rc = new_log_entry_iter.init(*log_file_);
        if (new_rc != RC::SUCCESS) {
            LOG_WARN("failed to init log entry iterator. rc=%s", strrc(new_rc));
            return new_rc;
        }

  // TODO 会不会跳过第一条日志？
  for (rc = new_log_entry_iter.next(); rc == RC::SUCCESS; rc = new_log_entry_iter.next()) {
    const LogEntry &log_entry = new_log_entry_iter.log_entry();
    switch (log_entry.log_type()) {
      case LogEntryType::MTR_BEGIN: {
      } break;

      case LogEntryType::MTR_COMMIT: {
        trx_manager->find_trx(log_entry.trx_id())->redo(db, log_entry);//重做事务
      } break;

      case LogEntryType::MTR_ROLLBACK: {
        trx_manager->find_trx(log_entry.trx_id())->redo(db, log_entry);//重做事务
      } break;

      case LogEntryType::INSERT: {
        if(trx_manager->find_trx(log_entry.trx_id()) != nullptr){
          trx_manager->find_trx(log_entry.trx_id())->redo(db, log_entry);//重做事务
        }
      } break;
      case LogEntryType::DELETE: {
        if(trx_manager->find_trx(log_entry.trx_id()) != nullptr){
          trx_manager->find_trx(log_entry.trx_id())->redo(db, log_entry);//重做事务
        }
      } break;
      case LogEntryType::ERROR: {
      } break;

      default: {
        LOG_WARN("unknown log type=%d", static_cast<int>(log_entry.log_type()));
      } break;
    }
  }

  trx_manager->next_trx_id();
  trx_manager->next_trx_id();
  int32_t next_trx_id =  trx_manager->next_trx_id();
  LOG_INFO("next trx id=%d", next_trx_id);



  return RC::SUCCESS;
}

