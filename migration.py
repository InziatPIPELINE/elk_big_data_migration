#s3_client = boto3.client('s3', aws_access_key_id='AKIAV3DM23DTXD2TCWCZ', aws_secret_access_key='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', region_name='ap-southeast-1')

from concurrent import futures
import os
import boto3

def download_dir(local, bucket):

    client = boto3.client('s3', aws_access_key_id='AKIAV3DM23DTXD2TCWCZ', aws_secret_access_key='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', region_name='ap-southeast-1')
    #print (client)

    def create_folder_and_download_file(k):
        dest_pathname = os.path.join(local, k)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
        print(f'downloading {k} to {dest_pathname}')
        client.download_file(bucket, k, dest_pathname)

    keys = []
    dirs = []
    next_token = ''
    base_kwargs = {
        'Bucket': bucket,
    }
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != '':
            kwargs.update({'ContinuationToken': next_token})
        results = client.list_objects_v2(**kwargs)
        print (results)
        contents = results.get('Contents')
        for i in contents:
            k = i.get('Key')
            if k[-1] != '/':
                keys.append(k)
            else:
                dirs.append(k)
        next_token = results.get('NextContinuationToken')
    for d in dirs:
        dest_pathname = os.path.join(local, d)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
    with futures.ThreadPoolExecutor() as executor:
        futures.wait(
            [executor.submit(create_folder_and_download_file, k) for k in keys],
            return_when=futures.FIRST_EXCEPTION,
        )


download_dir("migration_script/", "nins-side-elk")