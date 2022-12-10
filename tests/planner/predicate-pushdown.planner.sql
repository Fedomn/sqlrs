-- PushPredicateThroughJoin: pushdown to either side

select t1.* from t1 inner join t2 on t1.a=t2.b where t2.a > 2 and t1.a > 1

/*
original plan:
LogicalProject: exprs [t1.a:Nullable(Int64), t1.b:Nullable(Int64), t1.c:Nullable(Int64)]
  LogicalFilter: expr t2.a:Nullable(Int64) > Cast(2 as Int64) AND t1.a:Nullable(Int64) > Cast(1 as Int64)
    LogicalJoin: type Inner, cond On { on: [(t1.a:Nullable(Int64), t2.b:Nullable(Int64))], filter: None }
      LogicalTableScan: table: #t1, columns: [a, b, c]
      LogicalTableScan: table: #t2, columns: [a, b, c]

optimized plan:
PhysicalProject: exprs [t1.a:Nullable(Int64), t1.b:Nullable(Int64), t1.c:Nullable(Int64)]
  PhysicalHashJoin: type Inner, cond On { on: [(t1.a:Nullable(Int64), t2.b:Nullable(Int64))], filter: None }
    PhysicalFilter: expr t1.a:Nullable(Int64) > 1
      PhysicalTableScan: table: #t1, columns: [a, b, c]
    PhysicalProject: exprs [t2.b:Nullable(Int64)]
      PhysicalFilter: expr t2.a:Nullable(Int64) > 2
        PhysicalTableScan: table: #t2, columns: [a, b]
*/

-- PushPredicateThroughJoin: pushdown left outer join

select t1.* from t1 left join t2 on t1.a=t2.b where t2.a > 2 and t1.a > 1

/*
original plan:
LogicalProject: exprs [t1.a:Nullable(Int64), t1.b:Nullable(Int64), t1.c:Nullable(Int64)]
  LogicalFilter: expr t2.a:Nullable(Int64) > Cast(2 as Int64) AND t1.a:Nullable(Int64) > Cast(1 as Int64)
    LogicalJoin: type Left, cond On { on: [(t1.a:Nullable(Int64), t2.b:Nullable(Int64))], filter: None }
      LogicalTableScan: table: #t1, columns: [a, b, c]
      LogicalTableScan: table: #t2, columns: [a, b, c]

optimized plan:
PhysicalProject: exprs [t1.a:Nullable(Int64), t1.b:Nullable(Int64), t1.c:Nullable(Int64)]
  PhysicalFilter: expr t2.a:Nullable(Int64) > 2
    PhysicalProject: exprs [t1.a:Nullable(Int64), t1.b:Nullable(Int64), t1.c:Nullable(Int64), t2.a:Nullable(Int64)]
      PhysicalHashJoin: type Left, cond On { on: [(t1.a:Nullable(Int64), t2.b:Nullable(Int64))], filter: None }
        PhysicalFilter: expr t1.a:Nullable(Int64) > 1
          PhysicalTableScan: table: #t1, columns: [a, b, c]
        PhysicalTableScan: table: #t2, columns: [a, b]
*/

-- PushPredicateThroughJoin: pushdown right outer join

select t1.* from t1 right join t2 on t1.a=t2.b where t2.a > 2 and t1.a > 1

/*
original plan:
LogicalProject: exprs [t1.a:Nullable(Int64), t1.b:Nullable(Int64), t1.c:Nullable(Int64)]
  LogicalFilter: expr t2.a:Nullable(Int64) > Cast(2 as Int64) AND t1.a:Nullable(Int64) > Cast(1 as Int64)
    LogicalJoin: type Right, cond On { on: [(t1.a:Nullable(Int64), t2.b:Nullable(Int64))], filter: None }
      LogicalTableScan: table: #t1, columns: [a, b, c]
      LogicalTableScan: table: #t2, columns: [a, b, c]

optimized plan:
PhysicalProject: exprs [t1.a:Nullable(Int64), t1.b:Nullable(Int64), t1.c:Nullable(Int64)]
  PhysicalFilter: expr t1.a:Nullable(Int64) > 1
    PhysicalProject: exprs [t1.a:Nullable(Int64), t1.b:Nullable(Int64), t1.c:Nullable(Int64)]
      PhysicalHashJoin: type Right, cond On { on: [(t1.a:Nullable(Int64), t2.b:Nullable(Int64))], filter: None }
        PhysicalTableScan: table: #t1, columns: [a, b, c]
        PhysicalProject: exprs [t2.b:Nullable(Int64)]
          PhysicalFilter: expr t2.a:Nullable(Int64) > 2
            PhysicalTableScan: table: #t2, columns: [a, b]
*/

-- PushPredicateThroughJoin: pushdown common filters into join condition

select t1.* from t1 inner join t2 on t1.a=t2.b where t2.a > 2 and t1.a > t2.a

/*
original plan:
LogicalProject: exprs [t1.a:Nullable(Int64), t1.b:Nullable(Int64), t1.c:Nullable(Int64)]
  LogicalFilter: expr t2.a:Nullable(Int64) > Cast(2 as Int64) AND t1.a:Nullable(Int64) > t2.a:Nullable(Int64)
    LogicalJoin: type Inner, cond On { on: [(t1.a:Nullable(Int64), t2.b:Nullable(Int64))], filter: None }
      LogicalTableScan: table: #t1, columns: [a, b, c]
      LogicalTableScan: table: #t2, columns: [a, b, c]

optimized plan:
PhysicalProject: exprs [t1.a:Nullable(Int64), t1.b:Nullable(Int64), t1.c:Nullable(Int64)]
  PhysicalHashJoin: type Inner, cond On { on: [(t1.a:Nullable(Int64), t2.b:Nullable(Int64))], filter: Some(t1.a:Nullable(Int64) > t2.a:Nullable(Int64)) }
    PhysicalTableScan: table: #t1, columns: [a, b, c]
    PhysicalProject: exprs [t2.a:Nullable(Int64), t2.b:Nullable(Int64)]
      PhysicalFilter: expr t2.a:Nullable(Int64) > 2
        PhysicalTableScan: table: #t2, columns: [a, b]
*/

-- PushPredicateThroughJoin: don't pushdown filters for left outer join

select t1.* from t1 left join t2 on t1.a=t2.b where t2.a > 2 and t1.a > t2.a

/*
original plan:
LogicalProject: exprs [t1.a:Nullable(Int64), t1.b:Nullable(Int64), t1.c:Nullable(Int64)]
  LogicalFilter: expr t2.a:Nullable(Int64) > Cast(2 as Int64) AND t1.a:Nullable(Int64) > t2.a:Nullable(Int64)
    LogicalJoin: type Left, cond On { on: [(t1.a:Nullable(Int64), t2.b:Nullable(Int64))], filter: None }
      LogicalTableScan: table: #t1, columns: [a, b, c]
      LogicalTableScan: table: #t2, columns: [a, b, c]

optimized plan:
PhysicalProject: exprs [t1.a:Nullable(Int64), t1.b:Nullable(Int64), t1.c:Nullable(Int64)]
  PhysicalFilter: expr t2.a:Nullable(Int64) > 2 AND t1.a:Nullable(Int64) > t2.a:Nullable(Int64)
    PhysicalProject: exprs [t1.a:Nullable(Int64), t1.b:Nullable(Int64), t1.c:Nullable(Int64), t2.a:Nullable(Int64)]
      PhysicalHashJoin: type Left, cond On { on: [(t1.a:Nullable(Int64), t2.b:Nullable(Int64))], filter: None }
        PhysicalTableScan: table: #t1, columns: [a, b, c]
        PhysicalTableScan: table: #t2, columns: [a, b]
*/

-- PushPredicateThroughJoin: don't pushdown filters for right outer join

select t1.* from t1 right join t2 on t1.a=t2.b where t1.a > 2 and t1.a > t2.a

/*
original plan:
LogicalProject: exprs [t1.a:Nullable(Int64), t1.b:Nullable(Int64), t1.c:Nullable(Int64)]
  LogicalFilter: expr t1.a:Nullable(Int64) > Cast(2 as Int64) AND t1.a:Nullable(Int64) > t2.a:Nullable(Int64)
    LogicalJoin: type Right, cond On { on: [(t1.a:Nullable(Int64), t2.b:Nullable(Int64))], filter: None }
      LogicalTableScan: table: #t1, columns: [a, b, c]
      LogicalTableScan: table: #t2, columns: [a, b, c]

optimized plan:
PhysicalProject: exprs [t1.a:Nullable(Int64), t1.b:Nullable(Int64), t1.c:Nullable(Int64)]
  PhysicalFilter: expr t1.a:Nullable(Int64) > 2 AND t1.a:Nullable(Int64) > t2.a:Nullable(Int64)
    PhysicalProject: exprs [t1.a:Nullable(Int64), t1.b:Nullable(Int64), t1.c:Nullable(Int64), t2.a:Nullable(Int64)]
      PhysicalHashJoin: type Right, cond On { on: [(t1.a:Nullable(Int64), t2.b:Nullable(Int64))], filter: None }
        PhysicalTableScan: table: #t1, columns: [a, b, c]
        PhysicalTableScan: table: #t2, columns: [a, b]
*/

-- PushPredicateThroughNonJoin: pushdown filter with column alias

select t.a from (select * from t1 where a > 1) t where t.b > 7

/*
original plan:
LogicalProject: exprs [t.a:Nullable(Int64)]
  LogicalFilter: expr t.b:Nullable(Int64) > Cast(7 as Int64)
    LogicalProject: exprs [(t1.a:Nullable(Int64)) as t.a, (t1.b:Nullable(Int64)) as t.b, (t1.c:Nullable(Int64)) as t.c]
      LogicalFilter: expr t1.a:Nullable(Int64) > Cast(1 as Int64)
        LogicalTableScan: table: #t1, columns: [a, b, c]

optimized plan:
PhysicalProject: exprs [t.a:Nullable(Int64)]
  PhysicalProject: exprs [(t1.a:Nullable(Int64)) as t.a, (t1.b:Nullable(Int64)) as t.b, (t1.c:Nullable(Int64)) as t.c]
    PhysicalFilter: expr t1.b:Nullable(Int64) > 7 AND t1.a:Nullable(Int64) > 1
      PhysicalTableScan: table: #t1, columns: [a, b, c]
*/

