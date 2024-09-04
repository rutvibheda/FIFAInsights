from sqlalchemy import create_engine,text

engine=create_engine('postgresql+psycopg2://postgres:Mdirfan123@localhost/postgres')
conn=engine.connect()

table='player'
support=2
player='player'
clubname='club_name'
def execute_query(query):
    return conn.execute(text(query))

def getPlayers(count):
    return ",".join([f"p{i}.{player} as {player}{i}" for i in range(1,count+1)])

def getJoin(count):
    s=""
    for i in range(1,count):
        s+=f"JOIN {table} p{i+1} ON p{i+1}.{clubname}=p{i}.{clubname} AND p{i+1}.{player} > p{i}.{player}\n"
    return s

def create_lattice(max_level=5):
    level=1
    lastresult=None
    while level<=max_level:
        query=f"""
        select {getPlayers(level)},COUNT(DISTINCT p1.year) AS count from {table} p1
        {getJoin(level)}
        GROUP BY {",".join([f"p{i}.{player}" for i in range(1,level+1)])}
        HAVING COUNT(DISTINCT p1.year) >= {support};
        """

        result=execute_query(query)
        print(f"L{level}","-",result.rowcount)
        print(query)
        if result.rowcount==0:
            break
        level+=1
        lastresult=result
        
    return lastresult

def main():
    data=create_lattice()
    data=data.fetchall()
    for row in data:
        print(row)

if __name__=='__main__':
    main()