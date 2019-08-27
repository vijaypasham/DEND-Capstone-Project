import boto3
import configparser


def copy_files_to_s3():
	"""copies the json file to S3."""
	config = configparser.ConfigParser()
	config.read_file(open('dl.cfg'))
	
	AWS_ACCESS_KEY_ID = config.get('AWS', 'AWS_ACCESS_KEY_ID')
	AWS_SECRET_ACCESS_KEY = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
	AWS_BUCKET = config.get('S3', 'AWS_BUCKET')
	s3 = boto3.resource('s3',region_name='us-west-2',aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
	s3.meta.client.upload_file('world-happiness-report.json', AWS_BUCKET, 'world-happiness-report.json')
	
if __name__ == '__main__':
    copy_files_to_s3()