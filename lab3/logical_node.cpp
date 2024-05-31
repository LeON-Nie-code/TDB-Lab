#include "include/query_engine/planner/node/logical_node.h"

LogicalNode::~LogicalNode()
{}

void LogicalNode::add_child(std::unique_ptr<LogicalNode> oper)
{
  children_.emplace_back(std::move(oper));
}

void LogicalNode::swap_children()
{
  if (children_.size() == 2)
  {
    std::swap(children_[0], children_[1]);
  }
}
