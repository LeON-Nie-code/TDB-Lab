#pragma once

#include "project_tuple.h"
#include "row_tuple.h"
#include "tuple.h"
#include "values_tuple.h"

/**
 * @brief 将两个tuple合并为一个tuple
 * @ingroup Tuple
 * @details 在join算子中使用
 */
class JoinedTuple : public Tuple
{
public:
  JoinedTuple() = default;
  virtual ~JoinedTuple() = default;

  const TupleType tuple_type() const override { return JoinedTuple_Type; }

  void set_left(Tuple *left)
  {
    TupleType type = left->tuple_type();
    if (type == RowTuple_Type) {
      left_ = new RowTuple(*dynamic_cast<const RowTuple*>(left));
      left_ = dynamic_cast<Tuple*>(left);
    }
    if (type == ProjectTuple_Type) {
      left_ = new ProjectTuple(*dynamic_cast<const ProjectTuple*>(left));
      left_ = dynamic_cast<Tuple*>(left);
    }
    /*if (type == AggrTuple_Type) {
      left_ = new AggrTuple(*dynamic_cast<const AggrTuple*>(left));
      left_ = dynamic_cast<Tuple*>(left);
    }*/
    if (type == ValueListTuple_Type) {
      left_ = new ValueListTuple(*dynamic_cast<const ValueListTuple*>(left));
      left_ = dynamic_cast<Tuple*>(left);
    }
    if(type == JoinedTuple_Type)
    {
      left_ = new JoinedTuple(*dynamic_cast<const JoinedTuple*>(left));
      left_ = dynamic_cast<Tuple*>(left);
    }

  }
  void set_right(Tuple *right)
  {
    TupleType type = right->tuple_type();
        if (type == RowTuple_Type) {
          right_ = new RowTuple(*dynamic_cast<const RowTuple*>(right));
          right_ = dynamic_cast<Tuple*>(right);
        }
        if (type == ProjectTuple_Type) {
          right_ = new ProjectTuple(*dynamic_cast<const ProjectTuple*>(right));
          right_ = dynamic_cast<Tuple*>(right);
        }
        /*if (type == AggrTuple_Type) {
          right_ = new AggrTuple(*dynamic_cast<const AggrTuple*>(right));
          right_ = dynamic_cast<Tuple*>(right);
        }*/
        if (type == ValueListTuple_Type) {
          right_ = new ValueListTuple(*dynamic_cast<const ValueListTuple*>(right));
          right_ = dynamic_cast<Tuple*>(right);
        }
        if(type == JoinedTuple_Type)
        {
          right_ = new JoinedTuple(*dynamic_cast<const JoinedTuple*>(right));
          right_ = dynamic_cast<Tuple*>(right);
        }
  }

  void get_record(std::vector<Record *> &records) const override
  {
    left_->get_record(records);
    right_->get_record(records);
  }

  void set_record(std::vector<Record *> &records) override
  {
    left_->set_record(records);
    right_->set_record(records);
  }

  int cell_num() const override
  {
    return left_->cell_num() + right_->cell_num();
  }

  RC cell_at(int index, Value &value) const override
  {
    const int left_cell_num = left_->cell_num();
    if (index > 0 && index < left_cell_num) {
      return left_->cell_at(index, value);
    }

    if (index >= left_cell_num && index < left_cell_num + right_->cell_num()) {
      return right_->cell_at(index - left_cell_num, value);
    }

    return RC::NOTFOUND;
  }

  RC find_cell(const TupleCellSpec &spec, Value &value) const override
  {
    RC rc = left_->find_cell(spec, value);
    if (rc == RC::SUCCESS || rc != RC::NOTFOUND) {
      return rc;
    }

    return right_->find_cell(spec, value);
  }

private:
  Tuple *left_ = nullptr;
  Tuple *right_ = nullptr;
};