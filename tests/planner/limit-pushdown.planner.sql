-- LimitProjectTranspose: pushdown limit across project

select a from t1 offset 2 limit 1

/*
original plan:
LogicalLimit: limit Some(1), offset Some(2)
  LogicalProject: exprs [t1.a:Nullable(Int64)]
    LogicalTableScan: table: #t1, columns: [a, b, c]

optimized plan:
PhysicalTableScan: table: #t1, columns: [a], bounds: (offset:2,limit:1)
*/

-- PushLimitThroughJoin: don't pushdown limit when plan has order

select t1.a from t1 order by t1.b offset 1 limit 1

/*
original plan:
LogicalLimit: limit Some(1), offset Some(1)
  LogicalProject: exprs [t1.a:Nullable(Int64)]
    LogicalOrder: order [BoundOrderBy { expr: t1.b:Nullable(Int64), asc: true }]
      LogicalTableScan: table: #t1, columns: [a, b, c]

optimized plan:
PhysicalProject: exprs [t1.a:Nullable(Int64)]
  PhysicalLimit: limit Some(1), offset Some(1)
    PhysicalProject: exprs [t1.a:Nullable(Int64)]
      PhysicalOrder: Order [BoundOrderBy { expr: t1.b:Nullable(Int64), asc: true }]
        PhysicalTableScan: table: #t1, columns: [a, b]
*/

-- PushLimitThroughJoin: pushdown limit for left outer join

select t1.a from t1 left join t2 on t1.a=t2.b offset 1 limit 1

/*
original plan:
LogicalLimit: limit Some(1), offset Some(1)
  LogicalProject: exprs [t1.a:Nullable(Int64)]
    LogicalJoin: type Left, cond On { on: [(t1.a:Nullable(Int64), t2.b:Nullable(Int64))], filter: None }
      LogicalTableScan: table: #t1, columns: [a, b, c]
      LogicalTableScan: table: #t2, columns: [a, b, c]

optimized plan:
PhysicalProject: exprs [t1.a:Nullable(Int64)]
  PhysicalLimit: limit Some(1), offset Some(1)
    PhysicalProject: exprs [t1.a:Nullable(Int64)]
      PhysicalHashJoin: type Left, cond On { on: [(t1.a:Nullable(Int64), t2.b:Nullable(Int64))], filter: None }
        PhysicalTableScan: table: #t1, columns: [a], bounds: (offset:0,limit:2)
        PhysicalTableScan: table: #t2, columns: [b]
*/

-- PushLimitThroughJoin: pushdown limit for right outer join

select t1.a from t1 right join t2 on t1.a=t2.b limit 1

/*
original plan:
LogicalLimit: limit Some(1), offset None
  LogicalProject: exprs [t1.a:Nullable(Int64)]
    LogicalJoin: type Right, cond On { on: [(t1.a:Nullable(Int64), t2.b:Nullable(Int64))], filter: None }
      LogicalTableScan: table: #t1, columns: [a, b, c]
      LogicalTableScan: table: #t2, columns: [a, b, c]

optimized plan:
PhysicalProject: exprs [t1.a:Nullable(Int64)]
  PhysicalLimit: limit Some(1), offset None
    PhysicalProject: exprs [t1.a:Nullable(Int64)]
      PhysicalHashJoin: type Right, cond On { on: [(t1.a:Nullable(Int64), t2.b:Nullable(Int64))], filter: None }
        PhysicalTableScan: table: #t1, columns: [a]
        PhysicalTableScan: table: #t2, columns: [b], bounds: (offset:0,limit:1)
*/

-- PushLimitThroughJoin: don't push_limit_through_join when not contains limit

select t1.a from t1 right join t2 on t1.a=t2.b offset 10

/*
original plan:
LogicalLimit: limit None, offset Some(10)
  LogicalProject: exprs [t1.a:Nullable(Int64)]
    LogicalJoin: type Right, cond On { on: [(t1.a:Nullable(Int64), t2.b:Nullable(Int64))], filter: None }
      LogicalTableScan: table: #t1, columns: [a, b, c]
      LogicalTableScan: table: #t2, columns: [a, b, c]

optimized plan:
PhysicalProject: exprs [t1.a:Nullable(Int64)]
  PhysicalLimit: limit None, offset Some(10)
    PhysicalProject: exprs [t1.a:Nullable(Int64)]
      PhysicalHashJoin: type Right, cond On { on: [(t1.a:Nullable(Int64), t2.b:Nullable(Int64))], filter: None }
        PhysicalTableScan: table: #t1, columns: [a]
        PhysicalTableScan: table: #t2, columns: [b]
*/

