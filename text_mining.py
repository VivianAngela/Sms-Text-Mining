from __future__ import print_function
from pyspark import SparkContext, SparkConf



sc = SparkContext("local", "Simple App")

lines = sc.textFile("hdfs://master:9000/sms1.csv")

try:
	sms=lines.map(lambda p: p.lower())
	sms_result = sms.map(lambda p: sms_func(p))
except:
	pass

def gender(smstxt,sid,pno,t,fn):
	attribute="gender"
	if "mr." in smstxt or "dear sir" in smstxt or "dear salesperson" in smstxt or "hi sir" in smstxt or "hi uncle" in smstxt or "hi bhai" in smstxt or "hi bro" in smstxt or "hi father" in smstxt or "hi papa" in smstxt or "hi dad" in smstxt or "hi jiju" in smstxt or "hi mama" in smstxt or "hi chacha" in smstxt or "hi tau" in smstxt or "hi bhai" in smstxt or "hi ladke" in smstxt or "hi mr." in smstxt or "hey sir" in smstxt or "hey uncle" in smstxt or "hey bhaiya" in smstxt or "hey bro" in smstxt or "hey father" in smstxt or "hey papa" in smstxt or "hey dad" in smstxt or "hey jiju" in smstxt or "hey mama" in smstxt or "hey chacha" in smstxt or "hey tau" in smstxt or "hey bhai" in smstxt or "hey ladke" in smstxt or "hello sir" in smstxt or "hello uncle" in smstxt or "hello bhai" in smstxt or "hello bro" in smstxt or "hello father" in smstxt or "hello pa" in smstxt or "hello dad" in smstxt or "hello jiju" in smstxt or "hello mama" in smstxt or "hello chacha" in smstxt or "hello tau" in smstxt or "hello ladke" in smstxt or "aur mote" in smstxt or "aur londe" in smstxt or ("kumar" in smstxt and not "kumari" in smstxt) or "mohd" in smstxt or "mohamad" in smstxt or "muhammad" in smstxt:
		priority="1"
		val="male"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)

	if "hi mumma" in smstxt or "hi mummy" in smstxt or "hi mom" in smstxt or "hi di" in smstxt or "hi sis" in smstxt or "hi bua" in smstxt or "hi bhabhi" in smstxt or "hi behen" in smstxt or "hi aunty" in smstxt or "hi chachi" in smstxt or "hi ladki" in smstxt or "hi mam" in smstxt or "hi madam" in smstxt or "hey mumma" in smstxt or "hey mummy" in smstxt or "hey mom" in smstxt or "hey di" in smstxt or "hey sis" in smstxt or "hey bua" in smstxt or "hey bhabhi" in smstxt or "hey behen" in smstxt or "hey aunty" in smstxt or "hey chachi" in smstxt or "hey ladki" in smstxt or "hey mam" in smstxt or "hey madam" in smstxt or "dear mrs" in smstxt or "dear mam" in smstxt or "dear madam" in smstxt or "dear ms." in smstxt or "aur moti" in smstxt or "kumari" in smstxt or "kaur" in smstxt or "hi mrs" in smstxt or "hello mrs" in smstxt or "hey mrs" in smstxt or "hello ms." in smstxt or "hey ms." in smstxt or "hi ms." in smstxt: 
		priority="1"
		val="female"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)

	if smstxt.startswith("dear") and (not "dear customer" in smstxt or not "dear investor" in smstxt or not "dear counseling" in smstxt or not "dear valued" in smstxt or not "dear club" in smstxt or not "dear guest" in smstxt or not "dear incumbent" in smstxt or not "dear aviva" in smstxt or not "dear sir/mam" in smstxt or not "dear axis" in smstxt or not "dear partner" in smstxt or not "dear iterm" in smstxt or not "dear cbm/bm" in smstxt or not "dear msdian" in smstxt or not "dear csp" in smstxt or not "dear policyholder" in smstxt or not "dear channel" in smstxt or not "dear d2h" in smstxt or not "dear lvb" in smstxt or not "dear administrator" in smstxt or not "dear associate" in smstxt or not "dear dear parent" in smstxt or not "dear sir/madam" in smstxt or not "dear student" in smstxt or not "dear met" in smstxt or not "dear tmf" in smstxt or not "dear your" in smstxt or not "dear abcd" in smstxt or not "dear aegon" in smstxt or not "dear aspirant" in smstxt or not "dear asm" in smstxt or not "dear auditor" in smstxt or not "dear auro" in smstxt or not "dear bfl" in smstxt or not "dear bh" in smstxt or not "dear bussiness" in smstxt or not "dear candidates" in smstxt or not "dear cl84" in smstxt or not "dear crew" in smstxt or not "dear cust" in smstxt or not "dear dealor" in smstxt or not "dear deposit" in smstxt or not "dear dolphin" in smstxt or not "dear donor" in smstxt or not "dear dp" in smstxt or not "dear dotcabs" in smstxt or not "dear driver" in smstxt or not "dear emp" in smstxt or not "dear fac" in smstxt or not "dear fino" in smstxt or not "dear forum" in smstxt or not "dear friend" in smstxt or not "dear gamebuddy" in smstxt or not "dear gas" in smstxt or not "dear gsc" in smstxt or not "dear gold" in smstxt or not "dear health" in smstxt or not "dear guardian" in smstxt or not "dear health" in smstxt or not "dear host" in smstxt or not "dear indigo" in smstxt or not "dear instructor" in smstxt or not "dear jalan" in smstxt or not "dear key" in smstxt or not "dear kids" in smstxt or not "dear la" in smstxt or not "dear learner" in smstxt or not "dear fac" in smstxt or not "dear maben" in smstxt or not "dear member" in smstxt or not "dear passenger" in smstxt or not "dear pensioner" in smstxt or not "dear phama" in smstxt or not "dear prerana" in smstxt or not "dear principal" in smstxt or not "dear r.m" in smstxt or not "dear relationship" in smstxt or not "dear resident" in smstxt or not "dear retailer" in smstxt or not "dear rh" in smstxt or not "dear rm" in smstxt or not "dear rs name" in smstxt or not "dear rupantaran" in smstxt or not "dear seller" in smstxt or not "dear sir / madam" in smstxt or not "dear sm/oh" in smstxt or not "dear spice" in smstxt or not "dear so," in smstxt or not "dear sp," in smstxt or not "dear sri sai" in smstxt or not "dear staff" in smstxt or not "dear stockholding" in smstxt or not "dear subscriber" in smstxt or not "dear surveyor" in smstxt or not "dear teacher" in smstxt or not "dear manager" in smstxt or not "dear cavins" in smstxt or not "dear card" in smstxt or not "dear call" in smstxt or not "dear cabin" in smstxt or not "dear buisness" in smstxt or not "dear bsm" in smstxt or not "dear broker" in smstxt or not "dear branch" in smstxt or not "dear bm" in smstxt or not "dear beneficiary" in smstxt or not "dear bdm" in smstxt or not "dear bsc" in smstxt or not "dear arli" in smstxt or not "dear applicant" in smstxt or not "dear and8" in smstxt or not "dear and7" in smstxt or not "dear agr74" in smstxt or not "dear agent" in smstxt or not "dear adsf" in smstxt or not "dear advisor" in smstxt or not "dear admin" in smstxt or not "dear abc" in smstxt or not "dear __" in smstxt or not "dear <>" in smstxt or not "dear <<xyz>>" in smstxt or not "dear colleague" in smstxt or not "dear distributor" in smstxt or not "dear pnm" in smstxt or not "dear consumer" in smstxt or not "dear vp" in smstxt or not "dear trustline" in smstxt or not "dear travel" in smstxt or not "dear saving" in smstxt or not "dear reader" in smstxt or not "dear rbm" in smstxt or not "dear prof" in smstxt or not "dear patient" in smstxt or not "dear privilege" in smstxt or not "dear member" in smstxt or not "dear zh" in smstxt or not "dear xyz" in smstxt or not "dear xxx" in smstxt or not "dear viproite" in smstxt or not "dear visitor" in smstxt or not "dear vendor" in smstxt or not "dear vb" in smstxt or not "dear valuefirst" in smstxt or not "dear vaish" in smstxt or not "dear user" in smstxt or not "dear ucoites" in smstxt or not "dear ubiuser" in smstxt or not "dear trustee" in smstxt or not "dear trainer" in smstxt or not "dear tractor" in smstxt or not "dear tpddl" in smstxt or not "dear test" in smstxt or not "dear tmf" in smstxt or not "dear tech" in smstxt or not "dear team" in smstxt):
	    words=smstxt.split(" ")
	    if words[0]=="dear":
	    	name=words[1] 
	    	if name.isalpha():
	    		l=len(name)
	    		ch=name[l-1]
	    		if ch=='a' or ch=='e' or ch=='i' or ch=='o' or ch=='u':
	    			val="female"
	    			priority="2"
	    			print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
	    		else:
	    			val="male"
	    			priority="2"
	    			print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
	    	else:
	    		l=len(name)
	    		ch=name[l-2]
	    		if ch=='a' or ch=='e' or ch=='i' or ch=='o' or ch=='u':
	    			val="female"
	    			priority="2"
	    			print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
	    		else:
	    			val="male"
	    			priority="2"
	    			print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
	    			

	if smstxt.startswith("hi") and (not "hi loan" in smstxt or not "hi .y" in smstxt or not "hi thanks" in smstxt or not "hi ! y" in smstxt or not "hi hfrp" in smstxt or not "hi tssss" in smstxt or "hi this" in smstxt or not "hi there" in smstxt or not "hi the" in smstxt or not "hi (name)" in smstxt or not "hi -wel" in smstxt or not "hi - wel" in smstxt or not "hi - your" in smstxt):
		words=smstxt.split(" ")
		if words[0]=="hi":
			name=words[1] 
	    	if name.isalpha():
	    		l=len(name)
	    		ch=name[l-1]
	    		if ch=='a' or ch=='e' or ch=='i' or ch=='o' or ch=='u':
	    			val="female"
	    			priority="2"
	    			print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
	    		else:
	    			val="male"
	    			priority="2"
	    			print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
	    	else:
	    		l=len(name)
	    		ch=name[l-2]
	    		if ch=='a' or ch=='e' or ch=='i' or ch=='o' or ch=='u':
	    			val="female"
	    			priority="2"
	    			print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
	    		else:
	    			val="male"
	    			priority="2"
	    			print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
	    
	if smstxt.startswith("hello") and (not "hello, the" in smstxt or not "hello!we" in smstxt or not "hello!you" in smstxt or not "hello abcd" in smstxt or not "hello!subscribe" in smstxt or not "hello,we" in smstxt or "helloyour" in smstxt or not "hello! t" in smstxt or not "hello t" in smstxt or not "hello member" in smstxt or not "hello allotment" in smstxt or not "hello atm" in smstxt or not "hello, you" in smstxt or not "hello magzine" in smstxt or not "hello,you" in smstxt or not "hello, this" in smstxt or not "hello xyz" in smstxt or not "hello,a travel" in smstxt or not "hello, one" in smstxt or not "hello, kindly" in smstxt or not "hello. Please" in smstxt or not "hello, credit" in smstxt or not "hello@" in smstxt or not "hello, please" in smstxt or not "hello, item" in smstxt or not "hello from" in smstxt or not "hello, i" in smstxt or not "hello all" in smstxt or not "hello, your" in smstxt or not "hello!we" in smstxt or not "hello all" in smstxt or not "hello,as" in smstxt or not "hello. kindly" in smstxt):
		words=smstxt.split(" ")
		if words[0]=="hello":
			name=words[1] 
	    	if name.isalpha():
	    		l=len(name)
	    		ch=name[l-1]
	    		if ch=='a' or ch=='e' or ch=='i' or ch=='o' or ch=='u':
	    			val="female"
	    			priority="2"
	    			print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
	    		else:
	    			val="male"
	    			priority="2"
	    			print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
	    	else:
	    		l=len(name)
	    		ch=name[l-2]
	    		if ch=='a' or ch=='e' or ch=='i' or ch=='o' or ch=='u':
	    			val="female"
	    			priority="2"
	    			print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
	    		else:
	    			val="male"
	    			priority="2"
	    			print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)

def location(smstxt,sid,pno,t,circle):
	attribute="location"
	val="Yes"
	if circle=="Mumbai":
		priority="1"
		val="Mumbai"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)

	if circle=="Chennai":
		priority="1"
		val="Chennai"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)

	if circle=="Kolkata":
		priority="1"
		val="Kolkata"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)

	if "chandigarh" in smstxt:
		priority="1"
		val="chandigarh"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)


	data={'Maharashtra':['pune','nagpur'],'Rajasthan':['udaipur','jodhpur','jaisalmer','jaipur','ajmer','bikaner','kota'],'Karnataka':['bangalore','mysore','ooty'],'Bihar':['patna','muzzafarnagar'],'Punjab':['ludhiana','patiala','bhatinda','hoshiyarpur'],'Orissa':['bhubaneswar','cuttack'],'Tamilnadu':['coimbatore','madurai'],'WestBengal':['durgapur','howrah','darjeeling','siliguri','asansol','midnapore','bolpur','kalyani'],'Gujarat':['surat','gandhinagar','rajkot','ahmedabad'],'MadhyaPradesh':['bhopal','indore','jabalpur','gwalior','ujjain','ratlam','neemuch'],'Kerala':['thiruvananthapuram','kochi'],'AndhraPradesh':['hyderabad','vijayawada','secunderabad'],'Jammu':['srinagar','jammu'],'Assam':['guwahati','silchar','jorhat','tezpur','dibrugarh'],'UttarPradeshEast':['lucknow','kanpur','varanasi','gorakhpur','allahabad','ajamgarh','faizabad','moradabad','rampur','barelly','bareilly','jhansi','banaras','mathura','barabanki','sultanpur','saharanpur','sitapur','lakhimpur','pilibhit','hardoi']}	

	for i in data:
		if i==circle:
			for j in data[i]:
				if j in smstxt:
					val=j
					priority="2"
					print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)

				
def credit_card(smstxt,sid,pno,t):
	attribute="credit card"
	val="Yes"
	if "credit card" in smstxt or "creditcard" in smstxt:
		priority="1"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)

	if "card" in smstxt and ("payment" in smstxt or "receive" in smstxt or "changed" in smstxt or "blocked" in smstxt or "resolved" in smstxt):
		priority="2"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)

	if "card" in smstxt and "transaction" in smstxt and "declined":
		priority="2"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)


def car_loan(smstxt,sid,pno,t,fn):
	attribute="car_loan"
	val="Yes"
	if "car loan" in smstxt and ( not "thank you" in smstxt and not "clarifications" in smstxt):
		priority="1"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
		print (pno+"\t"+t+"\t"+"car_insurance"+"\t"+val+"\t"+priority)

	if "emi" in smstxt and ( not "emi card" in smstxt or not "credit card" in smstxt or not "cib" in smstxt or not "sip" in smstxt or not "gold" in smstxt or not "mutual fund" in smstxt or not "folio" in smstxt or not "bfl" in smstxt or not "bajaj finserv" in smstxt or not "pnbhfl" in smstxt or not "punjab housing" in smstxt or not "healthcover" in smstxt or not "exclusive offers" or not "tractor" in smstxt or not "bike" in smstxt or not "club mahindra" in smstxt or not "mercedes" in smstxt or not "emi preferred card" in smstxt or not "membership" in smstxt or not "holiday" in smstxt or not "muthoot" in smstxt or not "mortgage" in smstxt):
		priority="3"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
		print (pno+"\t"+t+"\t"+"car_insurance"+"\t"+val+"\t"+priority)

	if fn=="Maruti" or fn=="RBLBNK" or fn=="GMCBNK" or fn=="UnionB" or fn=="RATNAK" or fn=="CorpBk" or ("loan" in smstxt and "amount" in smstxt and "credited" in smstxt) or ("loan" in smstxt and "mature" in smstxt) or ("loan" in smstxt and "rate" in smstxt) or ("loan" in smstxt and "access") and (not "home loan" in smstxt):
		priority="2"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
		print (pno+"\t"+t+"\t"+"car_insurance"+"\t"+val+"\t"+priority)


def has_kids(smstxt,sid,pno,t,fn):
	attribute="has kids"
	val="Yes"
	if "dear parent" in smstxt or "your child" in smstxt or ("school" in smstxt and not "staff" in smstxt) or "quarter fee" in smstxt or "fee of your ward" in smstxt or "your ward" in smstxt:
		priority="1"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
		print (pno+"\t"+t+"\t"+"age"+"\t"+"35-45"+"\t"+priority)

	if fn=="ldcssms" or fn=="hanuman" or fn=="magnasoft" or fn=="netlink" or fn=="vj_vpsps" or fn=="stardotstarnon" or fn=="rimsmum":
		priority="1"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
		print (pno+"\t"+t+"\t"+"age"+"\t"+"35-45"+"\t"+priority)


def life_insurance(smstxt,sid,pno,t,fn):
	attribute="life_insurance"
	val="Yes"
	if "life insurance" in smstxt or "life policy" in smstxt or "pru policy" in smstxt or "indiafirst" in smstxt:
		priority="1"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)

	if fn=="SUDLIF":
		priority="1"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)


def health_insurance(smstxt,sid,pno,t,fn):
	attribute="health_insurance"
	val="Yes"
	if "health insurance" in smstxt or "medical insurance" in smstxt or "health policy" in smstxt or "indiafirst" in smstxt or "lombard policy":
		priority="1"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)

	if fn=="SUDLIF":
	 	priority="1"
	 	print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)


def net_banking(smstxt,sid,pno,t,fn):
	attribute="net_banking"
	val="Yes"
	if "internet banking" in smstxt or "inter net bkg." in smstxt or "net banking" in smstxt or "netbanking" in smstxt or "e_banking" in smstxt or "demat account" in smstxt:
		priority="1"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)

	if fn=="SUDLIF" or fn=="hanuman" or fn=="magnasoft" or fn=="netlink" or fn=="vj_vpsps" or fn=="stardotstarnon" or fn=="rimsmum":
		priority="1"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)

	    
def mutual_fund(smstxt,sid,pno,t,fn):
	attribute="life_insurance"
	val="Yes"
	if "mutual fund" in smstxt or " folio " in smstxt or "insurance" in smstxt or "mutualfund" in smstxt:
		priority="1"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)

	
def home_loan(smstxt,sid,pno,t,fn):
	attribute="home_loan"
	val="Yes"
	if "home loan" in smstxt or "loan account" in smstxt or "loan acct" in smstxt or "emi for acct" in smstxt or "emi for your loan" in smstxt or "vide receipt" in smstxt or "icici" in smstxt:
		priority="1"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
		print (pno+"\t"+t+"\t"+"savings_account"+"\t"+val+"\t"+priority)

	if "loan application" in smstxt and ("approved" in smstxt and not "auto approved") or "disbursement" in smstxt:
		priority="1"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
		print (pno+"\t"+t+"\t"+"savings_account"+"\t"+val+"\t"+priority)

	if "emi" in smstxt and ( not "emi card" in smstxt or not "credit card" in smstxt or not "cib" in smstxt or not "sip" in smstxt or not "gold" in smstxt or not "mutual fund" in smstxt or not "folio" in smstxt or not "bfl" in smstxt or not "bajaj finserv" in smstxt or not "bajaj auto" in smstxt or not "nano credit" in smstxt or not "tmf" in smstxt or not "bharatbenz" or not "capital first" in smstxt or not "tata motors" in smstxt or not "health cover" in smstxt or not "exclusive offers" in smstxt or not "tractor" in smstxt or not "membership" in smstxt or not "holiday" in smstxt or not "mercedes" in smstxt or not "emi preferred card" in smstxt or not "club mahindra" in smstxt or not "bike" in smstxt or not "auto" in smstxt):
		priority="2"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
		print (pno+"\t"+t+"\t"+"savings_account"+"\t"+val+"\t"+priority)

	if fn=="CorpBk" or fn=="RBLBNK" or fn=="GMCBNK" or fn=="UnionB" or fn=="RATNAK":
		priority="2"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)
		print (pno+"\t"+t+"\t"+"savings_account"+"\t"+val+"\t"+priority)

def age(smstxt,sid,pno,t,fn):
	attribute="age"
	
	if ("prepare" in smstxt and " mba " in smstxt) or "dear mba/pgdm aspirant" in smstxt or " freshers " in smstxt or "cat prep" in smstxt or "crack cat" in smstxt or "cat exam" in smstxt or ("admission open" in smstxt and "pg diploma" in smstxt):
		priority="1"
		val="18-25"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)

	if "child education plan" in smstxt:
		priority="1"
		val="45-50"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)

	if "admission open" in smstxt and "phd" in smstxt:
		priority="1"
		val="50+"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)

	if "senior citizens services" in smstxt:
		priority="2"
		val="25-30"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)

def car_insurance(smstxt,sid,pno,t,fn):
	attribute="car_insurance"
	val="Yes"
	if "maruti insurance" in smstxt:
		priority="1"
		print (pno+"\t"+t+"\t"+attribute+"\t"+val+"\t"+priority)


def sms_func(s):
	try:
		r=s.split("|")
		smsid=r[0]
		time=r[10]
		phoneno=r[3]
		userid=r[2]
		fromnumber=r[4]
		circle=r[9]
		smstxt=r[13]
		credit_card(smstxt,smsid,phoneno,time)
		car_loan(smstxt,smsid,phoneno,time,fromnumber)
		has_kids(smstxt,smsid,phoneno,time,fromnumber)
		life_insurance(smstxt,smsid,phoneno,time,fromnumber)
		health_insurance(smstxt,smsid,phoneno,time,fromnumber)
		mutual_fund(smstxt,smsid,phoneno,time,fromnumber)
		home_loan(smstxt,smsid,phoneno,time,fromnumber)
		net_banking(smstxt,smsid,phoneno,time,fromnumber)
		age(smstxt,smsid,phoneno,time,fromnumber)
		gender(smstxt,smsid,phoneno,time,fromnumber)
		location(smstxt,smsid,phoneno,time,circle)		
	except:
		pass
	

for i in sms_result.collect():
	i


