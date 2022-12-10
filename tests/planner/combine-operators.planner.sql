-- CollapseProject & CombineFilter: combine adjacent projects and filters into one

select t_2.* from (select t_1.* from (select * from t1 where c < 2) t_1 where t_1.a > 1) t_2 where t_2.b > 7;

/*
original plan:
LogicalProject: exprs [t_2.a:Nullable(Int64), t_2.b:Nullable(Int64), t_2.c:Nullable(Int64)]
  LogicalFilter: expr t_2.b:Nullable(Int64) > Cast(7 as Int64)
    LogicalProject: exprs [(t_1.a:Nullable(Int64)) as t_2.a, (t_1.b:Nullable(Int64)) as t_2.b, (t_1.c:Nullable(Int64)) as t_2.c]
      LogicalFilter: expr t_1.a:Nullable(Int64) > Cast(1 as Int64)
        LogicalProject: exprs [(t1.a:Nullable(Int64)) as t_1.a, (t1.b:Nullable(Int64)) as t_1.b, (t1.c:Nullable(Int64)) as t_1.c]
          LogicalFilter: expr t1.c:Nullable(Int64) < Cast(2 as Int64)
            LogicalTableScan: table: #t1, columns: [a, b, c]

optimized plan:
PhysicalProject: exprs [t_2.a:Nullable(Int64), t_2.b:Nullable(Int64), t_2.c:Nullable(Int64)]
  PhysicalProject: exprs [(t_1.a:Nullable(Int64)) as t_2.a, (t_1.b:Nullable(Int64)) as t_2.b, (t_1.c:Nullable(Int64)) as t_2.c]
    PhysicalProject: exprs [(t1.a:Nullable(Int64)) as t_1.a, (t1.b:Nullable(Int64)) as t_1.b, (t1.c:Nullable(Int64)) as t_1.c]
      PhysicalFilter: expr t1.b:Nullable(Int64) > 7 AND t1.a:Nullable(Int64) > 1 AND t1.c:Nullable(Int64) < 2
        PhysicalTableScan: table: #t1, columns: [a, b, c]
*/

