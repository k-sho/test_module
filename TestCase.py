from Postgres import Postgres
import sys


def test1():
    funcname = sys._getframe().f_code.co_name
    with open("result.log", 'a') as f:
        f.write(funcname)
        f.write("\n=======\n")

    db = Postgres("data")
    # db.set_pgpath('PG12')

    db.init()
    db.start()
    db.create_tablespace()
    db.alter_database("postgres")
    db.psql("postgres", "create table a(a int)")
    db.psql("postgres", "insert into a values(1)")
    db.dumpall("dumpfile.sql")
    db.drop_database("postgres")
    db.destroy()

    db.init()
    db.start()
    db.restore("dumpfile.sql")


test1()
