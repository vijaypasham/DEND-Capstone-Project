import configparser
import boto3
import json


def create_redshift_cluster():
	"""This scripts creates an instance of a Redshift cluster in AWS."""
	config = configparser.ConfigParser()
	config.read_file(open('dl.cfg'))
	AWS_ACCESS_KEY_ID = config.get('AWS', 'AWS_ACCESS_KEY_ID')
	AWS_SECRET_ACCESS_KEY = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
	DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
	DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
	DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
	DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
	DWH_DB = config.get("DWH", "DWH_DB")
	DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
	DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
	DWH_PORT = config.get("DWH", "DWH_PORT")
	DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
	
	iam = boto3.client('iam',aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_SECRET_ACCESS_KEY,region_name='us-west-2')
	
	print('Creating a new IAM Role')
	dwhRole = iam.create_role(
	Path='/', 
	RoleName=DWH_IAM_ROLE_NAME,
	Description='Allows Redshift clusters to call AWS services on your behalf.',
	AssumeRolePolicyDocument=json.dumps(
	    {
	        'Statement': [{
	            'Action': 'sts:AssumeRole',
	            'Effect': 'Allow',
	            'Principal': {'Service': 'redshift.amazonaws.com'}
	            }],
	            'Version': '2012-10-17'
	            }
	            )
	            )

	print('Attaching Policy')
	iam.attach_role_policy(
		RoleName=DWH_IAM_ROLE_NAME,
		PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
		)['ResponseMetadata']['HTTPStatusCode']
		
	print('Get the IAM role ARN')
	roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

	print(f"The roleArn is: {roleArn}")

	redshift = boto3.client('redshift', region_name='us-west-2', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
	response = redshift.create_cluster(
				ClusterType=DWH_CLUSTER_TYPE,
				NodeType=DWH_NODE_TYPE,
				NumberOfNodes=int(DWH_NUM_NODES),
				DBName=DWH_DB,
				ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
				MasterUsername=DWH_DB_USER,
				MasterUserPassword=DWH_DB_PASSWORD,
				IamRoles=[roleArn]
				)


if __name__ == '__main__':
    create_redshift_cluster()
			