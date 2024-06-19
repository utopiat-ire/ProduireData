using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using Npgsql;
using System.Data.Common;

namespace Produire.Data.PgSql
{
	[種類(DocUrl = "/data/db/dbpostgresql.htm")]
	public class PostgreSQLデータベース : データベースエンジン
	{
		#region フィールド

		string connectionString = null;
		NpgsqlConnectionStringBuilder constrBuilder = new NpgsqlConnectionStringBuilder();

		NpgsqlConnection connection = new NpgsqlConnection();
		NpgsqlTransaction transaction;

		#endregion

		public PostgreSQLデータベース()
		{
			constrBuilder.Host = "localhost";
			constrBuilder.Port = 5432;
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
			catch (NpgsqlException ex)
			{
				throw new ProduireException(ex);
			}
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

		public string 接続文字列
		{
			get { return constrBuilder.ConnectionString; }
			set
			{
				connectionString = value;
				constrBuilder.ConnectionString = connectionString;
			}
		}
		public string サーバ
		{
			get { return constrBuilder.Host; }
			set { constrBuilder.Host = value; }
		}
		public string ユーザ名
		{
			get { return constrBuilder.Username; }
			set { constrBuilder.Username = value; }
		}
		public string パスワード
		{
			get { return constrBuilder.Password; }
			set { constrBuilder.Password = value; }
		}
		public string データベース
		{
			get { return constrBuilder.Database; }
			set { constrBuilder.Database = value; }
		}
		public int ポート
		{
			get { return constrBuilder.Port; }
			set { constrBuilder.Port = value; }
		}
		public bool SSL使用
		{
			get { return constrBuilder.SslMode != SslMode.Disable; }
			set { constrBuilder.SslMode = value ? SslMode.Require : SslMode.Disable; }
		}
		public int 接続タイムアウト時間
		{
			get { return constrBuilder.Timeout; }
			set { constrBuilder.Timeout = value; }
		}
		public int 実行タイムアウト時間
		{
			get { return constrBuilder.CommandTimeout; }
			set { constrBuilder.CommandTimeout = value; }
		}
		public bool 接続プール使用
		{
			get { return constrBuilder.Pooling; }
			set { constrBuilder.Pooling = value; }
		}
		public int 最小プール数
		{
			get { return constrBuilder.MinPoolSize; }
			set { constrBuilder.MinPoolSize = value; }
		}
		public int 最大プール数
		{
			get { return constrBuilder.MaxPoolSize; }
			set { constrBuilder.MaxPoolSize = value; }
		}

		public string バージョン
		{
			get { return connection.ServerVersion; }
		}

		#endregion

		#region オーバロードされたメソッド

		[除外]
		public override IDbConnection Connection
		{
			get { return connection; }
		}

		[除外]
		public override DbDataAdapter GetDbDataAdapter(IDbCommand selectCommand)
		{
			NpgsqlDataAdapter adapter = new NpgsqlDataAdapter(selectCommand as NpgsqlCommand);
			return adapter;
		}
		[除外]
		public override DbCommandBuilder CreateCommandBuilder(DbDataAdapter adapter)
		{
			NpgsqlCommandBuilder builder = new NpgsqlCommandBuilder(adapter as NpgsqlDataAdapter);
			return builder;
		}

		[除外]
		public override IDbCommand CreateCommand(string query)
		{
			NpgsqlCommand command = new NpgsqlCommand(query, connection);
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

		#region サポートメソッド

		private string GetPostgreSqlEncoding(Encoding encoding)
		{
			if (encoding == Encoding.UTF8)
			{
				return "UTF8";
			}
			else if (encoding == Encoding.Unicode)
			{
				return "UNICODE";
			}
			else if (encoding.CodePage == 932)
			{
				return "SJIS";
			}
			else if (encoding.CodePage == 20932)
			{
				return "EUC_JP";
			}
			else
			{
				return "UTF8";
			}
		}

		#endregion
	}
}
