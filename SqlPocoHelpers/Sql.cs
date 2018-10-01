using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Entity;
using System.Data.Entity.Core.Objects;
using System.Data.SqlClient;
using System.Linq;
using PocoPropertyData;

namespace SqlPocoHelpers
{
    public static class Sql
    {
        public static SqlParameter Param(string name, object value)
        {
            return new SqlParameter(name, (value ?? DBNull.Value));
        }


        public static bool DoesFieldExist(SqlConnection conn, string tableName, string fieldName)
        {
            string SQL = "SELECT top 1 [" + fieldName + "] FROM [" + tableName + "]";
            try
            {
                var i = conn.SqlQueryScaler(SQL, null);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }



        public static object SqlQueryScaler(this Database db, string sql, List<Object> parameters)
        {
            if (parameters == null)
            {
                parameters = new List<object>();
            }
            db.DebugWrite(sql, parameters);
            var st = DateTime.Now;
            db.Log("-- Executing at " + DateTime.Now.ToShortDateString() + " " + DateTime.Now.ToShortTimeString());
            var ret = SqlQueryScaler((SqlConnection)db.Connection, sql, parameters);
            db.Log("-- Loaded in " + DateTime.Now.Subtract(st).TotalMilliseconds + " ms");
            return ret;
        }

        public static Object SqlQueryScaler(this SqlConnection db, string sql, List<Object> parameters)
        {
            try
            {

                if (parameters == null)
                {
                    parameters = new List<object>();
                }
                db.DebugWrite(sql, parameters);
                var conn = db;
                {
                    var cmd = new SqlCommand(sql, conn);
                    cmd.CommandTimeout = (db.ConnectionTimeout != 0 ? db.ConnectionTimeout : 20);

                    foreach (var param in parameters)
                    {
                        cmd.Parameters.Add(param);
                    }
                    return cmd.ExecuteScalar();
                }
            }
            catch (Exception ex)
            {
                var tstr = db.ArgsAsSql(sql, parameters);
                throw new Exception("Error: " + ex.Message + " on db call:" + tstr, ex);
            }
        }

        public static DataSet SqlQueryDataSet(this Database db, string sql, List<Object> parameters)
        {
            if (parameters == null)
            {
                parameters = new List<object>();
            }
            db.DebugWrite(sql, parameters);
            var st = DateTime.Now;
            db.Log("-- Executing at " + DateTime.Now.ToShortDateString() + " " + DateTime.Now.ToShortTimeString());
            var ret = SqlQueryDataSet((SqlConnection) db.Connection, sql, parameters);
            db.Log("-- Loaded in " + DateTime.Now.Subtract(st).TotalMilliseconds + " ms");
            return ret;
        }

        public static DataSet SqlQueryDataSet(this SqlConnection db, string sql, List<Object> parameters)
        {
            try
            {

                if (parameters == null)
                {
                    parameters = new List<object>();
                }

                db.DebugWrite(sql, parameters);
                var ds = new DataSet();
                var conn = db;
                {
                    var cmd = new SqlCommand(sql, conn);
                    cmd.CommandTimeout = (db.ConnectionTimeout != 0 ? db.ConnectionTimeout : 20);

                    foreach (var param in parameters)
                    {
                        cmd.Parameters.Add(param);
                    }

                    var da = new SqlDataAdapter(cmd);


                    da.Fill(ds);
                }
                return ds;
            }
            catch (Exception ex)
            {

                var tstr = db.ArgsAsSql(sql, parameters);

                throw new Exception("Error: " + ex.Message + " on db call:" + tstr, ex);
            }
        }

        public static DataTable SqlQueryTable(this SqlConnection db, string sql, List<Object> parameters)
        {
            return db.SqlQueryDataSet(sql, parameters).Tables[0];
        }

        public static DataTable SqlQueryTable(this Database db, string sql, List<Object> parameters)
        {
            return db.SqlQueryDataSet(sql, parameters).Tables[0];
        }


        public static void ExecuteSqlCommandExtended(this SqlConnection db, string sql,
            params SqlParameter[] parameters)
        {
            db.ExecuteSqlCommandExtended(sql, (from p in parameters select p as Object).ToList());
        }

        public static void ExecuteSqlCommandExtended(this Database db, string sql,
            params SqlParameter[] parameters)
        {
            db.ExecuteSqlCommandExtended(sql, (from p in parameters select p as Object).ToList());
        }

        public static void ExecuteSqlCommandExtended(this DbContext db, string sql,
            params SqlParameter[] parameters) 
        {
             db.Database.ExecuteSqlCommandExtended(sql, (from p in parameters select p as Object).ToList());
        }

        public static void ExecuteSqlCommandExtended(this SqlConnection db, string sql,
            List<Object> parameters = null)
        {
            db.SqlQueryDataSet(sql, parameters);
        }
        public static void ExecuteSqlCommandExtended(this DbContext db, string sql,
            List<Object> parameters = null)
        {

             db.Database.ExecuteSqlCommandExtended(sql, (from p in parameters select p as Object).ToList());
        }

        public static void ExecuteSqlCommandExtended(this Database db, string sql, List<Object> parameters = null)
        {
            var st = DateTime.Now;
            db.SqlQueryDataSet(sql, parameters);
            db.Log("-- Completed in " + DateTime.Now.Subtract(st).TotalMilliseconds + " ms");
        }
        public static IQueryable<TTt> SqlQueryExtended<TTt>(this SqlConnection db, string sql,
            List<SqlParameter> parameters = null, Dictionary<string, string> mappings = null) where TTt : class
        {
            if (mappings == null)
            {
                mappings = new Dictionary<string, string>();
            }
            var st = DateTime.Now;
            var tbl = db.SqlQueryTable(sql, (from p in parameters select (object)p).ToList());
            var st2 = DateTime.Now;
            var q = tbl.ToList<TTt>(columnMapping: mappings).AsQueryable();
            return q;
        }
        public static IQueryable<TTt> SqlQueryExtended<TTt>(this DbContext db, string sql,
            List<SqlParameter> parameters = null, Dictionary<string, string> mappings = null) where TTt : class
        {
            if (mappings == null)
            {
                mappings = new Dictionary<string, string>();
            }
            return db.Database.SqlQueryExtended<TTt>(sql, (from p in parameters select p as Object).ToList(), mappings);
        }


        public static IQueryable<TTt> SqlQueryExtended<TTt>(this Database db, string sql, List<Object> parameters = null, Dictionary<string, string> mappings = null)
            where TTt : class
        {
            if (mappings == null)
            {
                mappings = new Dictionary<string, string>();
            }
            var st = DateTime.Now;
            var tbl = db.SqlQueryTable(sql, parameters);
            var st2 = DateTime.Now;
            var q = tbl.ToList<TTt>(columnMapping:mappings).AsQueryable();
            db.Log("-- Mapping in " + DateTime.Now.Subtract(st2).TotalMilliseconds + " ms");
            db.Log("-- Completed in " + DateTime.Now.Subtract(st).TotalMilliseconds + " ms");
            return q;
        }


        public static Tuple<IQueryable<TT1>, IQueryable<TT2>> SqlQueryExtended<TT1, TT2>(this Database db,
            string sql, List<Object> parameters = null, Dictionary<string, string> mappings1 = null, Dictionary<string, string> mappings2 = null)
            where TT1 : class 
            where TT2 : class
        {
            mappings1 = mappings1 ?? new Dictionary<string, string>();
            mappings2 = mappings2 ?? new Dictionary<string, string>();
            var st = DateTime.Now;
            var ds = db.SqlQueryDataSet(sql, parameters);
            var st2 = DateTime.Now;
            IQueryable<TT1> q1 = null;
            IQueryable<TT2> q2 = null;
            if (ds.Tables.Count >= 1)
            {
                q1 = ds.Tables[0].ToList<TT1>(columnMapping: mappings1).AsQueryable();
            }
            if (ds.Tables.Count >= 2)
            {
                q2 = ds.Tables[1].ToList<TT2>(columnMapping: mappings2).AsQueryable();
            }
            var q = new Tuple<IQueryable<TT1>, IQueryable<TT2>>(q1, q2);
            db.Log("-- Mapping in " + DateTime.Now.Subtract(st2).TotalMilliseconds + " ms");
            db.Log("-- Completed in " + DateTime.Now.Subtract(st).TotalMilliseconds + " ms");
            return q;
        }
        public static Tuple<IQueryable<TT1>, IQueryable<TT2>, IQueryable<TT3>> SqlQueryExtended<TT1, TT2, TT3>(this Database db, string sql, List<Object> parameters = null, Dictionary<string, string> mappings1 = null, Dictionary<string, string> mappings2 = null, Dictionary<string, string> mappings3 = null)
            where TT1 : class
            where TT2 : class
            where TT3 : class
        {
            mappings1 = mappings1 ?? new Dictionary<string, string>();
            mappings2 = mappings2 ?? new Dictionary<string, string>();
            mappings3 = mappings3 ?? new Dictionary<string, string>();
            var st = DateTime.Now;
            var ds = db.SqlQueryDataSet(sql, parameters);
            var st2 = DateTime.Now;
            IQueryable<TT1> q1 = null;
            IQueryable<TT2> q2 = null;
            IQueryable<TT3> q3 = null;
            if (ds.Tables.Count >= 1)
            {
                q1 = ds.Tables[0].ToList<TT1>(columnMapping: mappings1).AsQueryable();
            }
            if (ds.Tables.Count >= 2)
            {
                q2 = ds.Tables[1].ToList<TT2>(columnMapping: mappings2).AsQueryable();
            }
            if (ds.Tables.Count >= 3)
            {
                q3 = ds.Tables[2].ToList<TT3>(columnMapping: mappings3).AsQueryable();
            }
            var q = new Tuple<IQueryable<TT1>, IQueryable<TT2>, IQueryable<TT3>>(q1, q2, q3);
            db.Log("-- Mapping in " + DateTime.Now.Subtract(st2).TotalMilliseconds + " ms");
            db.Log("-- Completed in " + DateTime.Now.Subtract(st).TotalMilliseconds + " ms");
            return q;
        }


        public static IQueryable<TTt> SqlQueryExtended<TTt>(this SqlConnection db, string sql, params object[] parameters) where TTt : class
        {
            var map = new Dictionary<string, string>();
            return db.SqlQueryExtended<TTt>(sql, (from p in parameters select p).ToList(), map);
        }


        public static IQueryable<TTt> SqlQueryExtended<TTt>(this DbContext db, string sql,
            params SqlParameter[] parameters) where TTt : class
        {
            return db.Database.SqlQueryExtended<TTt>(sql, (from p in parameters select p as Object).ToList());
        }
        public static IQueryable<TTt> SqlQueryExtended<TTt>(this Database db, string sql, params object[] parameters) where TTt : class
        {
            var map = new Dictionary<string, string>();
            return db.SqlQueryExtended<TTt>(sql, (from p in parameters select p).ToList(), map);
        }


        
        public static IQueryable<TTt> SqlQueryEx<TTt>(this DbContext db, string sql, Dictionary<string, string> mappings,
            params SqlParameter[] parameters) where TTt : class
        {
            return db.Database.SqlQueryExtended<TTt>(sql, parameters, mappings);
        }

        public static IQueryable<TTt> SqlQueryEx<TTt>(this Database db,
            string sql, Dictionary<string, string> mappings, params object[] parameters)
            where TTt : class
        {
            return db.SqlQueryExtended<TTt>(sql, (from p in parameters select p).ToList(), mappings);
        }
        public static IQueryable<TTt> SqlQueryEx<TTt>(this SqlConnection db,
            string sql, Dictionary<string, string> mappings, params object[] parameters)
            where TTt : class
        {
            return db.SqlQueryExtended<TTt>(sql, (from p in parameters select p).ToList(), mappings);
        }
    }
}
