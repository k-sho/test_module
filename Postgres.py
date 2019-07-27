import subprocess
import os
import shutil
from collections import defaultdict


class Postgres:
    def __init__(self, datadir):
        self.datadir = os.getcwd() + '/' + datadir
        self.tblspc = os.getcwd() + "/tblspc"
        self.init_pgpath()

    def init_pgpath(self):
        self.path_table = defaultdict(list)
        self.path_table["PG12"] = [
            "/Users/katoushou/dev/postgresql/bin", "/Users/katoushou/dev/postgresql/lib"]

    def get_pgpath(self, pgversion):
        return self.path_table[pgversion]

    def set_pgpath(self, pgversion):
        if os.environ.get('PATH') is None:
            os.environ['PATH'] = self.get_pgpath(pgversion)[0]
        else:
            os.environ['PATH'] = self.get_pgpath(
                pgversion)[0] + ':' + os.environ['PATH']

        if os.environ.get('LD_LIBRARY_PATH') is None:
            os.environ['LD_LIBRARY_PATH'] = self.get_pgpath(pgversion)[1]
        else:
            os.environ['LD_LIBRARY_PATH'] = self.get_pgpath(
                pgversion)[1] + ':' + os.environ['LD_LIBRARY_PATH']

    def init(self):
        subprocess.run(["initdb", "--no-locale", "-E",
                        "UTF8", "-D", self.datadir])

    def start(self):
        subprocess.run(["pg_ctl", "start", "-D", self.datadir])

    def restart(self):
        subprocess.run(["pg_ctl", "restart", "-D", self.datadir])

    def destroy(self):
        self.drop_tablespace()
        subprocess.run(["pg_ctl", "stop", "-D", self.datadir, "-m", "i"])
        os.rmdir(self.tblspc)
        shutil.rmtree(self.datadir)

    def dumpall(self, dumpfile, *options):
        cmd = ["pg_dumpall"]
        for opt in options:
            cmd.append(opt)
        proc = subprocess.run(cmd, stdout=subprocess.PIPE)
        with open(dumpfile, 'w') as f:
            f.write(proc.stdout.decode("utf8"))

    def restore(self, dumpfile):
        subprocess.run(["psql", "template1", "-f", dumpfile])

    def psql(self, dbname, query):
        cmd = ["psql", "-d", dbname, "-c", query]
        proc = subprocess.run(cmd, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT)

        with open("result.log", 'a') as f:
            f.write(proc.stdout.decode("utf8"))

        return proc.returncode

    def create_tablespace(self, **options):
        if not os.path.exists(self.tblspc):
            os.mkdir(self.tblspc)

        query = "create tablespace tblspc location \'%s\'" % self.tblspc

        if len(options) > 0:
            query += " with("
            i = 0
            for k, v in options.items():
                query += k + ' = ' + str(v)
                if i == len(options) - 1:
                    query += ')'
                    break
                query += ','
                i += 1
        self.psql("template1", query)

    def drop_tablespace(self):
        self.psql("template1", "drop tablespace tblspc")

    def drop_database(self, dbname):
        self.psql("template1", "drop database %s" % dbname)

    def alter_database(self, dbname):
        self.psql("template1", "alter database %s tablespace tblspc" % dbname)
