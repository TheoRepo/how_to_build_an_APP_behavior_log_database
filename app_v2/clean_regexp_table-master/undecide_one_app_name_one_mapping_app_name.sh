source /etc/profile
source  ~/.bash_profile


spark_sql_path=$1
label=$2


sql_part="

insert overwrite table profile${label}.dws_app_regexp_20211030 partition(version_no, the_date)
select 
app_name,
suspected_app_name,
posi_regexp,
nege_regexp,
case 
when app_name='360借呗' then '360借条'
when app_name='360借款' then '360借条'
when app_name='51人品' then '51人品贷'
when app_name='51生e金' then '人品贷极速版'
when app_name='58金融' then '58好借'
when app_name='FARFETCH' then 'FARFETCH'
when app_name='nice' then 'nice'
when app_name='一汽汽车金融' then '未找到'
when app_name='一汽租赁' then '未找到'
when app_name='万卡' then '万卡'
when app_name='万卡分期' then '万卡'
when app_name='世纪龙' then '世纪龙在线考试平台'
when app_name='中国移动-上海' then '中国移动'
when app_name='中国移动-务工行情' then '中国移动'
when app_name='中国移动-江西-办公' then '中国移动'
when app_name='中国移动-河南-办公' then '中国移动'
when app_name='中国移动-重庆' then '中国移动'
when app_name='中国联通-广东-办公' then '中国移动'
when app_name='世纪龙' then '世纪龙在线考试平台'
when app_name='中国移动-上海' then '中国移动'
when app_name='乐呗' then '乐呗'
when app_name='买单侠' then '买单侠'
when app_name='云南公安' then '云南公安'
when app_name='今日有钱' then '今日有钱'
when app_name='借呗' then '支付宝'
when app_name='借钱贷' then '借钱贷'
when app_name='分期白条' then '分期白条'
when app_name='分期还款' then '支付宝'
when app_name='前金服' then '前金服'
when app_name='加多宝' then '支付宝'
when app_name='北现金融' then '北现金融'
when app_name='北辰教育' then '北辰教育'
when app_name='卡贷' then '小赢卡贷'
when app_name='即分期' then '即有分期'
when app_name='即刻有' then '即刻有'
when app_name='友信普惠' then '友信普惠'
when app_name='友金普惠' then '友金普惠'
when app_name='合钱庄' then '合钱庄贷款'
when app_name='吉致汽车金融' then '吉致汽车金融'
when app_name='同程借钱' then '同程借钱'
when app_name='和信金融' then '和信金融'
when app_name='哈银消费金融' then '哈银消费金融'
when app_name='天猫养车' then '天猫养车'
when app_name='奔驰金融' then '奔驰金融'
when app_name='安心借钱' then '安心借钱'
when app_name='安心借钱' then '安心借钱'
when app_name='宜人小贷' then '宜人小贷'
when app_name='宜信公司' then '宜信公司'
when app_name='宝马金融' then '宝马金融'
when app_name='富盈金融' then '富盈金融'
when app_name='小鱼' then '小鱼易连'
when app_name='平安基金' then '平安基金'
when app_name='广汇汽车' then '广汇汽车金融'
when app_name='广汽汇理汽车金融' then '广汽汇理汽车金融'
when app_name='度小满' then '度小满金融'
when app_name='度小满有钱花' then '有钱花'
when app_name='建设银行' then '建设银行'
when app_name='循环贷' then '循环贷'
when app_name='微信支付' then '微信'
when app_name='微粒贷' then '微粒贷'
when app_name='度小满有钱花' then '有钱花'
when app_name='建设银行' then '建设银行'
when app_name='循环贷' then '循环贷'
when app_name='微信支付' then '微信'
when app_name='微粒贷' then '微粒贷'
when app_name='快贷宝' then '快贷宝'
when app_name='急速现金贷' then '极速现金贷'
when app_name='急速贷' then '极速贷'
when app_name='抱米花' then '抱米花'
when app_name='摩登时贷' then '摩登时贷'
when app_name='支付通' then '支付通'
when app_name='无锡农商银行' then '无锡农商银行'
when app_name='晋商消费金融' then '晋商消费金融'
when app_name='有银分期' then '有银分期'
when app_name='本地交友' then '未找到'
when app_name='本香世界' then '本香世界'
when app_name='来电助理' then '来电助理'
when app_name='来米伽' then '来米伽'
when app_name='来钱' then '来钱啦'
when app_name='杭银消费金融' then '杭银金融'
when app_name='洋钱罐借款' then '洋钱罐借款'
when app_name='海尔云贷' then '海尔云贷'
when app_name='海通恒信' then '恒信直通车'
when app_name='来钱' then '来钱啦'
when app_name='杭银消费金融' then '杭银金融'
when app_name='游侠客' then '游侠客旅行'
when app_name='潮信聊天' then '潮信'
when app_name='玖富' then '玖富'
when app_name='瑞福德金融' then '瑞福德'
when app_name='瑞蚨小贷' then '瑞蚨小贷'
when app_name='申万菱信基金' then '申万菱信基金'
when app_name='电子钱包' then '电子钱包'
when app_name='白条闪付' then '京东金融'
when app_name='白莲花' then '白莲花贷款'
when app_name='福特金融' then '福特金融'
when app_name='福贷' then '福贷'
when app_name='秋贝' then '秋贝金融'
when app_name='秒分' then '秒分贷款'
when app_name='给你花呗' then '给你花呗'
when app_name='维信卡卡贷' then '维信卡卡贷'
when app_name='网易' then 'CC直播'
when app_name='美利车金融' then '美利车金融'
when app_name='美图e钱包' then '美图e钱包'
when app_name='联联周边游' then '联联周边游'
when app_name='腾讯游戏' then '饭局狼人'
when app_name='臻有钱' then '哈啰出行'
when app_name='花鸭' then '花鸭'
when app_name='苏宁消费金融' then '苏宁消费金融'
when app_name='蚂蚁借呗' then '蚂蚁借呗'
when app_name='蚂蚁借呗' then '支付宝'
when app_name='蚂蚁微贷' then '蚂蚁微贷'
when app_name='蜗牛移动' then '蜗牛移动掌上营业厅'
when app_name='蜜约交友' then '蜜约交友'
when app_name='贝多分' then '贝多分'
when app_name='贵阳车展' then '贵阳车展'
when app_name='达融分期' then '达融分期'
when app_name='邮储银行' then '邮储银行'
when app_name='重庆燃气' then '重庆燃气'
when app_name='金美信' then '金美信消费金融'
when app_name='锦程消费金融' then '锦程消费金融'
when app_name='长城滨银' then '长城滨银汽车金'
when app_name='长安新生' then '长安新生马达贷'
when app_name='长安汽车金融' then '长安汽车金融'
when app_name='长安金融' then '长安汽车金融'
when app_name='长银五八消费金融' then '长银消费金融'
when app_name='随心用' then '中国移动'
when app_name='青岛啤酒旗舰店' then '手机天猫'
when app_name='麦芒科技' then '麦芒科技'
else mapping_app_name
end as mapping_app_name
app_index,
app_type,
type_index,
behavior,
behavior_index,
virtual_id,
version_no,
the_date
from profile${label}.dws_app_regexp_20211030_mid_7;
"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app" "$sql_part"

if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区 '${pt}'数据写入完成
