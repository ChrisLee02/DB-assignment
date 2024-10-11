# util funtion for readibility
def get_first_child_by_rule(tree, rule_name):
    for child in tree.children:
        if hasattr(child, "data") and child.data == rule_name:
            return child
    return None
