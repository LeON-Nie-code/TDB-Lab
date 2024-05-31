#pragma once

#include "include/query_engine/structor/expression/comparison_expression.h"
#include "include/query_engine/structor/tuple/join_tuple.h"
#include "physical_operator.h"

// TODO [Lab3] join算子的头文件定义，根据需要添加对应的变量和方法
class JoinPhysicalOperator : public PhysicalOperator
{
public:
  JoinPhysicalOperator();
  ~JoinPhysicalOperator() override = default;

  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::JOIN;
  }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;
  Tuple *current_tuple() override;

  std::unique_ptr<Expression> &condition()
  {
    return condition_;
  }
  void set_condition(std::unique_ptr<Expression> &&condition)
  {
    condition_ = std::move(condition);
  }
  void set_table_names(std::vector<char *> &&table_names)
  {
    table_names = std::move(table_names);
  }
  std::unique_ptr<ComparisonExpr> comparison_expr_;

  bool first_visit() const
  {
    return first_visit_;
  }
        void set_first_visit(bool first_visit)
        {
        first_visit_ = first_visit;
        }
private:
  Trx *trx_ = nullptr;
  JoinedTuple joined_tuple_;  //! 当前关联的左右两个tuple
  std::unique_ptr<Expression> condition_ = nullptr;  //! join的条件
  std::vector<char *> table_names;  //! join的表名
  bool first_visit_ = true;
};
