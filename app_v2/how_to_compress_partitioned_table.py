from string import Template

compress_query1 = '''merge table ga_test.interface_app_user_info_20210820 partition(${partitons}) options(compression='snappy')'''

partiiton_query1 = "show partitions ga_test.interface_app_user_info_20210820"


c_sql = Template(compress_query1)

def main(sparkSession):
    partitons = sparkSession.sql(partiiton_query1).collect()
    partitons_ = []
    for i in partitons:
        d = i[0].replace('/',"',").replace('=',"='") + "'"
        partitons_.append(d)
    for i in partitons_:
        _sql = c_sql.substitute(partitons = i)
        # print(_sql)
        sparkSession.sql(_sql)
