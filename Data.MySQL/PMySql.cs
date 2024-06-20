// 日本語プログラミング言語「プロデル」データベースプラグイン
// Copyright(C) 2007-2024 utopiat.net https://github.com/utopiat-ire/
using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;
using System.Data.Common;

namespace Produire.Data.MySQL
{
	[種類(DocUrl = "/data/db/dbmysql.htm")]
	public class MySQLデータベース : データベースエンジン
	{
		#region フィールド

		string connectionString;
		MySqlConnectionStringBuilder constrBuilder = new MySqlConnectionStringBuilder();
		Encoding encoding;

		MySqlConnection connection = new MySqlConnection();
		MySqlTransaction transaction;

		#endregion

		public MySQLデータベース()
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
			catch (MySqlException ex)
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


		/// <summary>データベースクエリを作ります。</summary>
		[自分で]
		public override データベースクエリ形 クエリを作る([という]string クエリ)
		{
			if (string.IsNullOrEmpty(クエリ)) return null;

			return new MySQLクエリ(this, CreateCommand(クエリ));
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
			get { return constrBuilder.Server; }
			set { constrBuilder.Server = value; }
		}
		public int ポート
		{
			get { return (int)constrBuilder.Port; }
			set { constrBuilder.Port = (uint)value; }
		}
		public string ユーザID
		{
			get { return constrBuilder.UserID; }
			set { constrBuilder.UserID = value; }
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
		public Encoding 文字コード
		{
			get { return encoding; }
			set
			{
				encoding = value;
				constrBuilder.CharacterSet = GetMySQLCharacterSet(encoding);
			}
		}
		public int 接続タイムアウト時間
		{
			get { return (int)constrBuilder.ConnectionTimeout; }
			set { constrBuilder.ConnectionTimeout = (uint)value; }
		}
		public int 実行タイムアウト時間
		{
			get { return (int)constrBuilder.DefaultCommandTimeout; }
			set { constrBuilder.DefaultCommandTimeout = (uint)value; }
		}
		public bool 接続プール使用
		{
			get { return constrBuilder.Pooling; }
			set { constrBuilder.Pooling = value; }
		}
		public int 最小プール数
		{
			get { return (int)constrBuilder.MinimumPoolSize; }
			set { constrBuilder.MinimumPoolSize = (uint)value; }
		}
		public int 最大プール数
		{
			get { return (int)constrBuilder.MaximumPoolSize; }
			set { constrBuilder.MaximumPoolSize = (uint)value; }
		}

		public string バージョン
		{
			get { return connection.ServerVersion; }
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
			MySqlDataAdapter adapter = new MySqlDataAdapter(selectCommand as MySqlCommand);
			return adapter;
		}
		[除外]
		public override DbCommandBuilder CreateCommandBuilder(DbDataAdapter adapter)
		{
			MySqlCommandBuilder builder = new MySqlCommandBuilder(adapter as MySqlDataAdapter);
			return builder;
		}

		[除外]
		public override IDbCommand CreateCommand(string query)
		{
			MySqlCommand command = new MySqlCommand(query, connection);
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

		protected override IDbDataParameter CreateParameter(IDbCommand command, object value)
		{
			MySqlParameter p = ((MySqlCommand)command).CreateParameter();
			p.Value = value;
			p.ResetDbType();
			return p;
		}

		#endregion

		#region サポートメソッド

		private string GetMySQLCharacterSet(Encoding encoding)
		{
			if (encoding == Encoding.UTF8)
			{
				return "utf8";
			}
			else if (encoding.CodePage == 932)
			{
				return "sjis";
			}
			else if (encoding.CodePage == 20932)
			{
				return "ujis";
			}
			else
			{
				return "utf8";
			}
		}

		#endregion

	}

	[種類(DocUrl = "/data/db/dbmysql.htm")]
	public class MySQLクエリ : データベースクエリ形
	{
		private MySqlCommand dbCommand;

		public MySQLクエリ(データベースエンジン engine, IDbCommand dbCommand)
			: base(engine)
		{
			this.dbCommand = dbCommand as MySqlCommand;
		}

		public override IDbCommand DbCommand
		{
			get { return dbCommand; }
		}

		public long 最終挿入ID
		{
			get { return dbCommand.LastInsertedId; }
		}
	}
}
