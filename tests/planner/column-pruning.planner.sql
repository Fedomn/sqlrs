-- PushProjectIntoTableScan: column pruning into table scan

select a from t1

/*
original plan:
LogicalProject: exprs [t1.a:Int64]
  LogicalTableScan: table: #t1, columns: [a, b, c]

optimized plan:
PhysicalTableScan: table: #t1, columns: [a]
*/

-- PushProjectThroughChild: column pruning across aggregate

select sum(b)+1 from t1 where a > 1

/*
original plan:
LogicalProject: exprs [Sum(t1.b:Int64):Int64 + Cast(1 as Int64)]
  LogicalAgg: agg_funcs [Sum(t1.b:Int64):Int64] group_by []
    LogicalFilter: expr t1.a:Int64 > Cast(1 as Int64)
      LogicalTableScan: table: #t1, columns: [a, b, c]

optimized plan:
PhysicalProject: exprs [Sum(t1.b:Int64):Int64 + 1]
  PhysicalSimpleAgg: agg_funcs [Sum(t1.b:Int64):Int64] group_by []
    PhysicalProject: exprs [t1.b:Int64]
      PhysicalFilter: expr t1.a:Int64 > 1
        PhysicalTableScan: table: #t1, columns: [a, b]
*/

-- RemoveNoopOperators: column pruning remove unused projection

select sum(b) from t1 where a > 1

/*
original plan:
LogicalProject: exprs [Sum(t1.b:Int64):Int64]
  LogicalAgg: agg_funcs [Sum(t1.b:Int64):Int64] group_by []
    LogicalFilter: expr t1.a:Int64 > Cast(1 as Int64)
      LogicalTableScan: table: #t1, columns: [a, b, c]

optimized plan:
PhysicalSimpleAgg: agg_funcs [Sum(t1.b:Int64):Int64] group_by []
  PhysicalProject: exprs [t1.b:Int64]
    PhysicalFilter: expr t1.a:Int64 > 1
      PhysicalTableScan: table: #t1, columns: [a, b]
*/

-- PushProjectThroughChild: column pruning across join

select t1.a, t2.b from t1 left join t2 on t1.a = t2.a where t2.b > 1

/*
original plan:
LogicalProject: exprs [t1.a:Int64, t2.b:Int64]
  LogicalFilter: expr t2.b:Int64 > Cast(1 as Int64)
    LogicalJoin: type Left, cond On { on: [(t1.a:Int64, t2.a:Int64)], filter: None }
      LogicalTableScan: table: #t1, columns: [a, b, c]
      LogicalTableScan: table: #t2, columns: [a, b, c]

optimized plan:
PhysicalProject: exprs [t1.a:Int64, t2.b:Int64]
  PhysicalFilter: expr t2.b:Int64 > 1
    PhysicalProject: exprs [t1.a:Int64, t2.b:Nullable(Int64)]
      PhysicalHashJoin: type Left, cond On { on: [(t1.a:Int64, t2.a:Int64)], filter: None }
        PhysicalTableScan: table: #t1, columns: [a]
        PhysicalTableScan: table: #t2, columns: [a, b]
*/

-- PushProjectThroughChild: column pruning across multiple join

select employee.id, employee.first_name, department.department_name, state.state_name, state.state_code from employee 
left join department on employee.department_id=department.id
right join state on state.state_code=employee.state;  

/*
original plan:
LogicalProject: exprs [employee.id:Int64, employee.first_name:Utf8, department.department_name:Utf8, state.state_name:Utf8, state.state_code:Utf8]
  LogicalJoin: type Right, cond On { on: [(employee.state:Nullable(Utf8), state.state_code:Utf8)], filter: None }
    LogicalJoin: type Left, cond On { on: [(employee.department_id:Nullable(Int64), department.id:Int64)], filter: None }
      LogicalTableScan: table: #employee, columns: [id, first_name, last_name, state, job_title, salary, department_id]
      LogicalTableScan: table: #department, columns: [id, department_name]
    LogicalTableScan: table: #state, columns: [id, state_code, state_name]

optimized plan:
PhysicalProject: exprs [employee.id:Int64, employee.first_name:Utf8, department.department_name:Utf8, state.state_name:Utf8, state.state_code:Utf8]
  PhysicalHashJoin: type Right, cond On { on: [(employee.state:Nullable(Utf8), state.state_code:Utf8)], filter: None }
    PhysicalProject: exprs [employee.id:Int64, employee.first_name:Utf8, employee.state:Nullable(Utf8), department.department_name:Nullable(Utf8)]
      PhysicalHashJoin: type Left, cond On { on: [(employee.department_id:Nullable(Int64), department.id:Int64)], filter: None }
        PhysicalTableScan: table: #employee, columns: [id, first_name, state, department_id]
        PhysicalTableScan: table: #department, columns: [id, department_name]
    PhysicalTableScan: table: #state, columns: [state_code, state_name]
*/

