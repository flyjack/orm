##About 

![server-nado/orm](./logo.jpg) 

一个数据库ORM.

## How to use?

### Insert 
go get github.com/server-nado/orm



## Super database

sqlite3 "github.com/mattn/go-sqlite3"
mysql "github.com/go-sql-driver/mysql"
postgree "github.com/lib/pq"

##数据库Model 建立方法

    //引用模块
    import "github.com/server-nado/orm"

    //mysql 驱动
    import _ "github.com/go-sql-driver/mysql"
    
    //建立连接 
    // 参数分别为 名称 ， 驱动， 连接字符串
    // 注：必须包含一个default 连接， 作为默认连接。
    orm.NewDatabase("default" , "mysql" , "user:passwd@ip/database?charset=utf8&parseTime=true")
	


    //建立一个数据模型。 
	type UserInfo struct**** {
		orm.Object
		Id int64 `field:"id" auto:"true" index:"pk"`
		Name string `field:"username"`
		Passwd string `field:"password"`
	}
	
	
	
	//读写分离:
	orm.NewDatabase("write-conname" , "mysql" , "user:passwd@ip/database?charset=utf8&parseTime=true")
	orm.NewDatabase("read-conname" , "mysql" , "user:passwd@ip/database?charset=utf8&parseTime=true")
	orm.SetWriteConnectName("write-conname")
	orm.SetReadConnectName("read-conname")
	
	
	
	//变更Cache 列表(Redis)
	
	orm.CacheConsistent.Add("127.0.0.1:6379") //添加一个redis服务地址
	orm.CacheConsistent.Set([]string{"127.0.0.1:6379" , "127.0.0.1:6380"})   //批量设置redis 
	orm.CacheConsistent.Remove("127.0.0.1:6380") //删除一个地址
	
	

[更多信息>>](docs/mode.md)

##新增 CacheModel 模型， 支持分布式redis作为数据库缓存。 

	import "github.com/server-nado/orm"
	import _ "github.com/go-sql-driver/mysql"

	type userB struct {
		CacheModule
		Uid     int64  `field:"Id" index:"pk" cache:"user" `
		Alias   string `field:"Alias"`
		Money int64  `field:"money"	`
	}

	func main(){
		orm.CacheConsistent.Add("127.0.0.1:6379")  //添加多个redis服务器
		orm.SetCachePrefix("nado") //默认nado .  将作为redis key 的前缀
		NewDatabase("default", "mysql", "happy:passwd@tcp(127.0.0.1:3306)/mydatabase?charset=utf8&parseTime=true")


		b := new(userB)
		b.Uid = 10000
		err:=b.Objects(b).One()
		if err!= nil {
			panic(err)
		}
		fmt.Println(b.Uid ,b.Alias ,b.Money)

		b.Incrby("Money" , 100)
		fmt.Println(b.Money)
		b.Save() //不执行不会保存到数据库 只会修改redis数据。 


	}
	
##一些说明， 这个ORM的目的：

1. 简化数据库操作方式
2. 降低数据库的操作压力

比较适合出现频繁，高性能要求的数据库读写操作。 使用CacheModule 模式操作数据时， 大多数情况下，热数据会被导入到缓存，这样的情况下， mysql的度频率会降低，读速度会提高很多，同时， 使用Set或者Incry等方式修改数据， 写速度会大大提升。