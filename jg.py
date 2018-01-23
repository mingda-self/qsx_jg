import tushare as ts 
import datetime
from sqlalchemy import create_engine
import numpy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import MySQLdb
import smtplib
from email.mime.text import MIMEText
from email.header import Header
engine = create_engine('mysql://root:idm@1234@rm-uf60zv8oe0nvf29g7o.mysql.rds.aliyuncs.com/qsx_jg?charset=utf8')

start_date='2017-01-23'
#1、计算沪深两市a股两年内的均线数据，并存入数据库
#2、记录当前交易日前60个交易日的收盘价，以便以后计算新的均线值
def initMA(securitys,step,cacheKData):
	# pass
	# security = ts.get_stock_basics()
	# print(securitys['code'])
	data=[] #list of dict

	# row_name=[]
	# column_name=[]
	for code in securitys.iloc[:,0]:
		if code.startswith('600') or code.startswith('601') or code.startswith('603') or code.startswith('000'):	#只取沪深a股
			# row_name.append(code)		#行为证券代码
			if cacheKData.get(code) is not None:
				df=cacheKData[code]
				initSecurityMA(df,step,data,code)
			else:
				for i in range(0,5):
					df = ts.get_k_data(code,ktype='D',start=start_date, end=datetime.date.today().strftime('%Y-%m-%d'))
					if df.empty == False:
						break

				if df.empty:
					print('%s:get_k_data return empty'%code)
					continue
				# print(df)
				cacheKData[code] = df
				initSecurityMA(df,step,data,code,True)

	# print(data, row_name,column_name)
	dt_ma = pd.DataFrame(data)

	# print(dt_ma)
	if engine is not None:
		print('to_sql ma_%d'%step)
		dt_ma.to_sql('ma_%d'%step,engine, if_exists='replace')
		# print(dt_ma)

def initSecurityMA(df,step,data,code,store_close=False):
	print('enter initSecurityMA')
	pre_close_price_list = []
	
	try:
		size = df.iloc[:,0].size
	except Exception as e:
		print(df)
		return

	closePrice=[]
	# size = df.iloc(:,0).size
	for index in range(0,size):
		ma_date = df.iloc[index, 0]
		close_price = df.iloc[index,2]
		high_price = df.iloc[index,3]
		low_price = df.iloc[index,4]
		pre_close_price_list.append(close_price)
		if store_close:
			close_price_item={}
			close_price_item['code']=code
			close_price_item['high']=float('%.2f'%high_price)
			close_price_item['low']=float('%.2f'%low_price)
			close_price_item['close']=float('%.2f'%close_price)
			close_price_item['date']='%s'%ma_date
			closePrice.append(close_price_item)
		if len(pre_close_price_list) < step:
			continue
		else:
			
			data_item = {}
			data_item['code'] = code
			data_item['date'] = '%s'%ma_date
			# column_name.append(df.iloc[index, 0])	#列为日期
			narray=numpy.array(pre_close_price_list)
			ma=narray.sum()/step
			data_item['ma'] = float('%.2f'%ma)
			del pre_close_price_list[0]		#删除第一个元素
			# print(ma)
			data.append(data_item)
	# print(data_item)
	if store_close:
		dt_close = pd.DataFrame(closePrice)
		if engine is not None:
			# print('to_sql close_price')
			dt_close.to_sql('security_k_data',engine, if_exists='append')
#初始加载a股的均线数据
def initAShareMa():

	# securitys = ts.get_stock_basics()
	securitys = pd.read_sql_query('select code from ref_data', con = engine)

	# securitys.to_sql('ref_data',engine, if_exists='append')
	cacheKData={}
	print('begin MA5')
	initMA(securitys,5,cacheKData)
	print('begin MA10')
	initMA(securitys,10,cacheKData)
	print('begin MA15')
	initMA(securitys,15,cacheKData)
	print('begin MA20')
	initMA(securitys,20,cacheKData)
	print('begin MA30')
	initMA(securitys,30,cacheKData)
	print('begin MA60')
	initMA(securitys,60,cacheKData)
	print('end initAShareMa')
# print(date.today().strftime('%Y-%m-%d'))
# df = ts.get_k_data('601688',ktype='D',start='2017-10-01', end=date.today().strftime('%Y-%m-%d'))
#定时每天早上8点执行
#每日追加上一个交易日的a股均线数据
#从数据库读取前最新的step-1条收盘价，然后查询并存储前一个交易日的收盘价，计算得到前一个交易日的均值，并存储
def appendAShareMa(step,store_close=False):
	securitys = pd.read_sql_query('select code from ref_data', con = engine)
	closePrice = []
	ma_data=[]
	

	preTradeDate = datetime.date.today() + datetime.timedelta(days=-1)
	strPreTradeDate = preTradeDate.strftime('%Y-%m-%d')
	for code in securitys['code']:
		close_price_dt = pd.read_sql_query('select close,date from security_k_data where code=%s order by date desc limit %d'%(code,step -1), con = engine)
		if close_price_dt.empty:
			print('code:%s is not exist'%code)
			data=[]
			df = ts.get_k_data(code,ktype='D',start=start_date, end=strPreTradeDate)

			if df.empty:
				print('%s:get_k_data return empty'%code)
				continue

			initSecurityMA(df,step,data,code,True)

			dt_ma_old = pd.DataFrame(data)

			if engine is not None:
				print('to_sql ma_%d'%step)
				dt_ma_old.to_sql('ma_%d'%step,engine, if_exists='append')


		if close_price_dt.empty:
			close_price_dt = pd.read_sql_query('select close,date from security_k_data where code=%s order by date desc limit %d'%(code,step -1), con = engine)
		if close_price_dt.empty:
			continue

		DBNewTradeDate = close_price_dt.iloc[0,1]	#数据库记录的最新日期
		

		if strPreTradeDate == DBNewTradeDate:
			continue


		df = ts.get_k_data(code,ktype='D',start=strPreTradeDate, end=datetime.date.today().strftime('%Y-%m-%d'))

		if df.empty:
			print('%s:appendAShareMa get_k_data return empty'%code)
			continue

		ma_date = df.iloc[0, 0]
		close_price = df.iloc[0,2]
		high_price = df.iloc[0,3]
		low_price = df.iloc[0,4]
		if store_close:
			close_price_item={}
			close_price_item['code']=code
			close_price_item['high']=float('%.2f'%high_price)
			close_price_item['low']=float('%.2f'%low_price)
			close_price_item['close']=float('%.2f'%close_price)
			close_price_item['date']='%s'%ma_date
			closePrice.append(close_price_item)

		#计算ma
		totalClosePrice = 0.0
		for close_price_old in close_price_dt['close']:
			totalClosePrice = totalClosePrice + close_price_old

		totalClosePrice = totalClosePrice + close_price
		ma = totalClosePrice/step

		ma_data_item={}
		ma_data_item['code']=code
		ma_data_item['ma']=float('%.2f'%ma)
		ma_data_item['date']='%s'%ma_date
		ma_data.append(ma_data_item)

	#存储ma
	dt_ma = pd.DataFrame(ma_data)
	if engine is not None:
		print('to_sql append ma')
		dt_ma.to_sql('ma_%d'%step,engine, if_exists='append')

	if store_close:
		dt_close = pd.DataFrame(closePrice)
		if engine is not None:
			print('to_sql append close_price')
			dt_close.to_sql('security_k_data',engine, if_exists='append')
		#计算均值
		
# print(df)




def appendMaTimely():
	appendAShareMa(5,True)
	appendAShareMa(10)
	appendAShareMa(15)
	appendAShareMa(20)
	appendAShareMa(30)
	appendAShareMa(60)
#均线策略，获取满足stepMin上穿stepMax的股票列表（买入列表）和stepMin下穿stepMax（卖出列表）并返回
def mas_getStockListMatchCase(stepMin=5, stepMax=30):
	lstBuy=[]
	lstSell=[]

	#返回证券及近15日价格高点和低点和均价
	#卖出策略，stepMin下穿stepMax
	securitys_dt = pd.read_sql_query('select code from ref_data', con = engine)
	if securitys_dt.empty:
		return lstBuy, lstSell

	for code in securitys_dt['code']:
		maMin_dt = pd.read_sql_query('select ma from ma_%d where code=%s order by date desc limit 10'%(stepMin,code), con = engine)
		maMax_dt = pd.read_sql_query('select ma from ma_%d where code=%s order by date desc limit 10'%(stepMax,code), con = engine)
		#买入策略，stepMax上穿stepMax
		#判断上穿，条件是如果maMin_dt['ma'][0] > maMax_dt['ma'][0] && maMin_dt['ma'][1] < maMax_dt['ma'][1]
		if maMin_dt.empty or maMax_dt.empty:
			continue

		if maMin_dt['ma'][0] > maMax_dt['ma'][0]:
			if maMin_dt['ma'][1] < maMax_dt['ma'][1]:
				item={}
				item['code']=code
				close_dt = pd.read_sql_query('select close,high,low from security_k_data where code=%s order by date desc limit 15'%(code), con = engine)
				if close_dt.empty == False:

					item['high'] = close_dt['high'].max()
					item['low'] = close_dt['low'].min()
					item['average']=float('%.2f'%close_dt['close'].mean())
					
				# print(item)

				lstBuy.append(str(item))

		if maMin_dt['ma'][0] < maMax_dt['ma'][0]:
			if maMin_dt['ma'][1] > maMax_dt['ma'][1]:
				item={}
				item['code']=code
				item['pre_close']=code
				close_dt = pd.read_sql_query('select close,high,low from security_k_data where code=%s order by date desc limit 15'%(code), con = engine)
				if close_dt.empty == False:

					item['high'] = close_dt['high'].max()
					item['low'] = close_dt['low'].min()
					item['average']=float('%.2f'%close_dt['close'].mean())
					
				# print(item)

				lstSell.append(str(item))


	return lstBuy, lstSell

def jobTimely():
	# securitys = ts.get_stock_basics()
	# if securitys.empty == False:
	# 	db = MySQLdb.connect("rm-uf60zv8oe0nvf29g7o.mysql.rds.aliyuncs.com","root","idm@1234","qsx_jg" )
	# 	cursor = db.cursor()
	# 	cursor.execute("delete from ref_data")
	# 	db.commit()
	# 	db.close()
	# 	securitys.to_sql('ref_data',engine, if_exists='append')

	# appendMaTimely()
	#找出符合条件的股票代码，并邮件发送
	lstBuy,lstSell = mas_getStockListMatchCase()
	htmlText = 'buy code list:\n'
	htmlText = htmlText + '\n'.join(lstBuy)
	htmlText = htmlText + '\n\n'
	htmlText = htmlText + 'sell code list:\n'
	htmlText = htmlText + '\n'.join(lstSell)

	print(htmlText)
	# print(lstBuy,lstSell)
	SendEmail('',htmlText)


def SendEmail(attachfile, htmlText,fromAdd='952574701@qq.com', toAdd='804959964@qq.com', subject='qsx_jg'):
	strFrom = fromAdd;
	strTo = toAdd;
	msg =MIMEText(htmlText);
	msg['Content-Type'] = 'Text/HTML';
	msg['Subject'] = Header(subject,'gb2312');
	msg['To'] = strTo;
	msg['From'] = strFrom;

	smtp = smtplib.SMTP('smtp.qq.com');
	smtp.login('952574701@qq.com','qxm19890116');
	try:
		smtp.sendmail(strFrom,strTo,msg.as_string());
	finally:
		smtp.close;
# initAShareMa()


jobTimely()