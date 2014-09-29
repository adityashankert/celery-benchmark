from django.http import HttpResponse
from anyjson import serialize

from celery import task
import mysql.connector
import MySQLdb

@task
def queue(request):
	queue_name = request.GET['queue']
	cnx = mysql.connector.connect(user='root', password='',host='127.0.0.1',database='test')
	cnx1 = mysql.connector.connect(user='root', password='',host='127.0.0.1',database='test')
	cursor = cnx.cursor()
	
	query = "SELECT SchoolId,Phone,Message,DateTime,Status,UserId,Role FROM {}".format(queue_name)
	cursor.execute(query)
	rows = cursor.fetchall()
	
	
	for (SchoolId,Phone,Message,DateTime,Status,UserId,Role) in rows:
		cursor_back = cnx1.cursor()
		query_back = "INSERT INTO `queue_back` (SchoolId, Phone, Message, DateTime, Status, UserId, Role, id) VALUES ('{}', '{}', '{}', CURRENT_TIMESTAMP, '1', '{}', '{}', NULL)".format(str(SchoolId),str(Phone),MySQLdb.escape_string(Message.encode('utf-8')),str(UserId),Role)
		cursor_back.execute(query_back)
	cnx.close()
	cnx1.commit()
	cnx1.close()
	response = {'status': 'success'}
	return HttpResponse(serialize(response), mimetype='application/json')
	
