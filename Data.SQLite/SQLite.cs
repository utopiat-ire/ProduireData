using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SQLite;
using System.Data;
using System.Data.Common;

namespace Produire.Data.SQLite
{
	[種類(DocUrl = "/data/db/dbsqlite.htm")]
	public class SQLiteデータベース : データベースエンジン
	{
		#region フィールド

		string connectionString;
		SQLiteConnectionStringBuilder constrBuilder = new SQLiteConnectionStringBuilder();

		SQLiteConnection connection = new SQLiteConnection();
		SQLiteTransaction transaction;

		#endregion

		public SQLiteデータベース()
		{
		}

		#region 手順

		/// <summary>データベースサーバへ接続します</summary>
		[自分へ]
		public override void 接続([で, 省略]string 接続文字列)
		{
			if (string.IsNullOrEmpty(接続文字列))
			{
				if (!string.IsNullOrEmpty(connectionString))
				{
					connection.ConnectionString = connectionString;
				}
				else
				{
					connection.ConnectionString = constrBuilder.ConnectionString;
				}
			}
			else
			{
				connection.ConnectionString = 接続文字列;
			}

			try
			{
				connection.Open();
			}
			catch (Exception ex)
			{
				throw new ProduireException(ex);
			}
		}

		/// <summary>拡張機能を登録します</summary>
		[自分へ, 手順名("登録する")]
		public void 登録する([から, 省略]string DLLファイル名, [を, 省略]string 関数名)
		{
			if (connection.State != ConnectionState.Open)
			{
				throw new ProduireException("拡張機能を登録する前に接続してください。");
			}
			if (DLLファイル名 == null) DLLファイル名 = "SQLite.Interop.dll";
			if (string.IsNullOrEmpty(関数名))
				connection.LoadExtension(DLLファイル名);
			else
				connection.LoadExtension(DLLファイル名, 関数名);
		}

		/// <summary>トランザクション処理を開始します</summary>
		[自分で, 手順名("トランザクションを", "開始")]
		public override トランザクション トランザクションを開始()
		{
			if (connection.State == ConnectionState.Open)
			{
				transaction = connection.BeginTransaction();
				return new トランザクション(transaction);
			}
			else
			{
				throw new ProduireException("データベースへ接続されていません。");
			}
		}
		/// <summary>トランザクション処理をコミットして処理を完了させます</summary>
		[自分で, 手順名("トランザクションを", "完了")]
		public override void トランザクションを完了()
		{
			if (transaction != null)
			{
				transaction.Commit();
			}
			else
			{
				throw new ProduireException("トランザクションが開始されていません。");
			}
		}
		/// <summary>トランザクション処理をロールバックして処理を取り消します</summary>
		[自分で, 手順名("トランザクションを", "取り消す")]
		public override void トランザクションを取り消す()
		{
			if (transaction != null)
			{
				transaction.Rollback();
			}
			else
			{
				throw new ProduireException("トランザクションが開始されていません。");
			}
		}
		#endregion

		#region 設定項目

		public string データソース
		{
			get { return constrBuilder.DataSource; }
			set { constrBuilder.DataSource = value; }
		}
		public string 接続文字列
		{
			get { return constrBuilder.ConnectionString; }
			set
			{
				connectionString = value;
				constrBuilder.ConnectionString = connectionString;
			}
		}
		public string バージョン
		{
			get { return connection.ServerVersion; }
		}
		public bool 拡張機能有効
		{
			set { connection.EnableExtensions(value); }
		}

		#endregion

		#region オーバロード

		[除外]
		public override IDbConnection Connection
		{
			get { return connection; }
		}

		[除外]
		public override DbDataAdapter GetDbDataAdapter(IDbCommand selectCommand)
		{
			SQLiteDataAdapter adapter = new SQLiteDataAdapter(selectCommand as SQLiteCommand);
			return adapter;
		}
		[除外]
		public override DbCommandBuilder CreateCommandBuilder(DbDataAdapter adapter)
		{
			SQLiteCommandBuilder builder = new SQLiteCommandBuilder(adapter as SQLiteDataAdapter);
			return builder;
		}

		[除外]
		public override IDbCommand CreateCommand(string query)
		{
			SQLiteCommand command = new SQLiteCommand(query, connection);
			if (transaction != null) command.Transaction = transaction;
			return command;
		}

		[除外]
		public override string[] GetTableNames()
		{
			bool keepAlive = (connection.State == ConnectionState.Open);
			if (!keepAlive) 接続(null);

			List<string> names = new List<string>();
			DataTable table = connection.GetSchema("Tables");
			foreach (DataRow row in table.Rows)
			{
				if (row["TABLE_TYPE"].ToString().ToUpper() != "TABLE") continue;
				names.Add(row["TABLE_NAME"].ToString());
			}

			if (!keepAlive) 切断();
			return names.ToArray();
		}

		[除外]
		public override string[] GetViewNames()
		{
			bool keepAlive = (connection.State == ConnectionState.Open);
			if (!keepAlive) 接続(null);

			List<string> names = new List<string>();
			DataTable table = connection.GetSchema("Views");
			foreach (DataRow row in table.Rows)
			{
				if (row["TABLE_TYPE"].ToString().ToUpper() != "TABLE") continue;
				names.Add(row["TABLE_NAME"].ToString());
			}

			if (!keepAlive) 切断();
			return names.ToArray();
		}

		[除外]
		public override string[] GetColumnNames(string tableName)
		{
			List<string> names = new List<string>();
			DataTable table = connection.GetSchema("Columns", new string[] { null, null, tableName });
			foreach (DataRow row in table.Rows)
			{
				names.Add(row["column_name"].ToString());
			}
			return names.ToArray();
		}

		#endregion
	}
}
