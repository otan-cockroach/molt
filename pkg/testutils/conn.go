package testutils

import "os"

func PGConnStr() string {
	pgInstanceURL := "postgres://postgres:postgres@localhost:5432/testdb"
	if override, ok := os.LookupEnv("POSTGRES_URL"); ok {
		pgInstanceURL = override
	}
	return pgInstanceURL
}

func CRDBConnStr() string {
	crdbInstanceURL := "postgres://root@127.0.0.1:26257/defaultdb?sslmode=disable"
	if override, ok := os.LookupEnv("COCKROACH_URL"); ok {
		crdbInstanceURL = override
	}
	return crdbInstanceURL
}

func MySQLConnStr() string {
	mysqlInstanceURL := "jdbc:mysql://root@tcp(localhost:3306)/defaultdb"
	if override, ok := os.LookupEnv("MYSQL_URL"); ok {
		mysqlInstanceURL = override
	}
	return mysqlInstanceURL
}
