import subprocess
import os
from collections import defaultdict

class Postgres:
    def __init__(self, datadir):
        self.datadir = os.getcwd() + '/' + datadir
        self.tblspc = os.getcwd() + "/tblspc"
        self.init_pgpath()

    def init_pgpath(self):
        self.path_table = defaultdict(list)
        self.path_table["PG12"] = ["/Users/katoushou/dev/postgresql/bin",
        "/Users/katoushou/dev/postgresql/lib"]
    
    def get_pgpath(self, pgversion):
        return self.path_table[pgversion]

    def set_pgpath(self, pgversion):
        if os.environ.get('PATH') is None: 
            os.environ['PATH'] = self.get_pgpath(pgversion)[0]
        else:
            os.environ['PATH'] = self.get_pgpath(pgversion)[0] + ':' + os.environ['PATH']
        
        if os.environ.get('LD_LIBRARY_PATH') is None:
            os.environ['LD_LIBRARY_PATH'] = self.get_pgpath(pgversion)[1]
        else:
            os.environ['LD_LIBRARY_PATH'] = self.get_pgpath(pgversion)[1] + ':' + os.environ['LD_LIBRARY_PATH']
        
        print(os.environ['PATH'])
        print()
        print(os.environ['LD_LIBRARY_PATH'])

    def init(self):
        subprocess.run(["initdb", "--no-locale", 
        "-E", "UTF8", "-D",self.datadir])

    def start(self):
        subprocess.run(["pg_ctl", "start", "-D", self.datadir])
    
    def restart(self):
        subprocess.run(["pg_ctl", "restart", "-D", self.datadir])

    def dumpall(self, dumpfile, *args):
        cmd = ["pg_dumpall"]
        for arg in args:
            cmd.append(arg)
        proc = subprocess.run(cmd, stdout = subprocess.PIPE)
        with open(dumpfile, 'w') as f:
            f.write(proc.stdout.decode("utf8"))

    def psql(self, query):
        cmd = ["psql", "-t", "-d", "template1", "-c", query]
        subprocess.run(cmd)

    def create_tablespace(self, **kwargs):
        if not os.path.exists(self.tblspc):
            os.mkdir(self.tblspc)
        
        query = "create tablespace tblspc location \'%s\'" % self.tblspc 
        query += " with("
        
        i = 0
        for k, v in kwargs.items():
            query += k + ' = ' + str(v)
            if i == len(kwargs) - 1:
                query += ')'
                break
            query += ','
            i+=1
        print(query)
        self.psql(query)

db = Postgres("data")
db.set_pgpath("PG12")
db.psql("select version()")
db.init()
db.start()