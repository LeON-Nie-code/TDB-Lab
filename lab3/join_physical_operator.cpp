#include "include/query_engine/planner/operator/join_physical_operator.h"

#include "include/query_engine/structor/expression/conjunction_expression.h"

/* TODO [Lab3] join的算子实现，需要根据join_condition实现Join的具体逻辑，
  最后将结果传递给JoinTuple, 并由current_tuple向上返回
 JoinOperator通常会遵循下面的被调用逻辑：
 operator.open()
 while(operator.next()){
    Tuple *tuple = operator.current_tuple();
 }
 operator.close()
*/

JoinPhysicalOperator::JoinPhysicalOperator() = default;

// 执行next()前的准备工作, trx是之后事务中会使用到的，这里不用考虑
RC JoinPhysicalOperator::open(Trx *trx)
{
        trx_ = trx;
        ExprType temp = condition_->type();
        auto* conjunction_expr = dynamic_cast<ConjunctionExpr *>(condition_.get());
        ExprType temp2 = conjunction_expr->children()[0]->type();
        if(temp2 == ExprType::COMPARISON && comparison_expr_ == nullptr)
        {
          comparison_expr_ = std::unique_ptr<ComparisonExpr>(dynamic_cast<ComparisonExpr *>(conjunction_expr->children()[0].get()));
        }
        else
        {
                return RC::MY_ERROR;
        }
        children_[0]->open(trx_);
        children_[1]->open(trx_);

  return RC::SUCCESS;
}

// 计算出接下来需要输出的数据，并将结果set到join_tuple中
// 如果没有更多数据，返回RC::RECORD_EOF
RC JoinPhysicalOperator::next()
{
  if(children_.empty())
    return RC::RECORD_EOF;

  RC rc_0;
  RC rc_1;
  Value left_value;
  Value right_value;
  bool result = false;
  if (first_visit())
        {
    children_[0]->next();
    children_[1]->next();
        set_first_visit(false);
        children_[1]->close();
        children_[1]->open(trx_);
        }
  vector<Record *> records_0;
  vector<Record *> records_1;
  children_[0]->current_tuple()->get_record(records_0);
  children_[1]->current_tuple()->get_record(records_1);
  if(records_0[0]->data() == nullptr || records_1[0]->data() == nullptr)
    return RC::RECORD_EOF;
  while(true)
  {
    while((rc_1 = children_[1]->next()) == RC::SUCCESS)
    {
      comparison_expr_->left()->get_value(*children_[0]->current_tuple(), left_value);
      comparison_expr_->right()->get_value(*children_[1]->current_tuple(), right_value);
      LOG_INFO("tuple_str: %s, %s", children_[0]->current_tuple()->to_string().c_str(), children_[1]->current_tuple()->to_string().c_str());
      LOG_INFO("left_value: %s, right_value: %s", left_value.get_string().c_str(), right_value.get_string().c_str());
      if(left_value.compare(right_value) == 0)
      {
        joined_tuple_.set_left(children_[0]->current_tuple());
        joined_tuple_.set_right(children_[1]->current_tuple());
        return RC::SUCCESS;
      }
    }
    if(rc_1 == RC::RECORD_EOF)
    {
      LOG_INFO("rc_1 == RECORD_EOF");
      rc_0 = children_[0]->next();
      children_[1]->close();
      children_[1]->open(trx_);
      if(rc_0 == RC::RECORD_EOF)
      {
        return RC::RECORD_EOF;
      }
    }
  }

}

// 节点执行完成，清理左右子算子
RC JoinPhysicalOperator::close()
{
  if(children_.empty())
    return RC::SUCCESS;
        children_[0]->close();
        children_[1]->close();
  return RC::SUCCESS;
}

Tuple *JoinPhysicalOperator::current_tuple()
{
  return &joined_tuple_;
}
