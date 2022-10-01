-- CollapseProject & CombineFilter: combine adjacent projects and filters into one

select * from (select * from (select * from t1 where c < 2) where a > 1) where b > 7;

/*
original plan:
LogicalProject: exprs [t1.a:Int64, t1.b:Int64, t1.c:Int64]
  LogicalFilter: expr t1.b:Int64 > Cast(7 as Int64)
    LogicalProject: exprs [t1.a:Int64, t1.b:Int64, t1.c:Int64]
      LogicalFilter: expr t1.a:Int64 > Cast(1 as Int64)
        LogicalProject: exprs [t1.a:Int64, t1.b:Int64, t1.c:Int64]
          LogicalFilter: expr t1.c:Int64 < Cast(2 as Int64)
            LogicalTableScan: table: #t1, columns: [a, b, c]

optimized plan:
PhysicalProject: exprs [t1.a:Int64, t1.b:Int64, t1.c:Int64]
  PhysicalFilter: expr t1.b:Int64 > 7 AND t1.a:Int64 > 1 AND t1.c:Int64 < 2
    PhysicalTableScan: table: #t1, columns: [a, b, c]
*/

