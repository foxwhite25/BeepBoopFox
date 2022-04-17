## BB. Fox

在 QQ 频道上运行的个人机器人。

## Running

如果可以的话尽量不要构造一个完全一样的机器人，此处的源代码锦用于 qq.py 的学习目的。

如果你只想要其中一个cog的功能，完全可以把cog单独拿出来使用，这样你也不需要一个数据库。

尽管如此，整个bot的安装步骤如下：

1. **确保你是 Python 3.8 或更高版本**

这是实际运行机器人所必需的。

2. **设置虚拟环境**

运行 `python3.8 -m venv venv`

3. **安装依赖**

运行 `pip install -U -r requirements.txt`

4. **在 PostgreSQL 中创建数据库**

你将需要 PostgreSQL 9.5 或更高版本，并在 `psql` 中键入以下内容:

```sql
CREATE ROLE bb_fox WITH LOGIN PASSWORD 'yourpw';
CREATE DATABASE bb_fox OWNER bb_fox;
CREATE EXTENSION pg_trgm;
```

5. **设置配置**

下一步就是使用以下模板在机器人所在的根目录中创建一个 `config.py` 文件:

```py
bots_app_id = '' # 你的 app id
bots_token = '' # 你的 token
postgresql = 'postgresql://user:password@host/database' # 你的 postgresql 信息
```

6. **数据库配置**

要配置 PostgreSQL 数据库供机器人使用，请 ``cd`` 到 ``launcher.py`` 所在的目录，然后通过执行 ``python3 launcher.py db init`` 运行脚本