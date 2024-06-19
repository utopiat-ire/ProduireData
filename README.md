# プロデルデータベースプラグイン

プロデルから各種データベースエンジンにアクセスするためのプラグインです。

データベースエンジンはコネクタなどが頻繁に更新されるため、
利用時点での最新版のコネクタを利用できるように、ライブラリを自分で更新する目的で共有します。

また、多くのライブラリが.NET Framework Data Providersに基づいて作られているため
このソースを応用して、プロデルから独自のデータベースエンジンへの接続に対応させることもできます。