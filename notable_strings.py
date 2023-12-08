Account_ID = 584739742957

user_ID = '1272e2b5acdf'

zookeeper_connection = "z-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:2181,z-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:2181,z-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:2181"

boostrap_brokers = "b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098"

bucket_name = 'user-1272e2b5acdf-bucket'

invoke_url = 'https://dapwwc6yy2.execute-api.us-east-1.amazonaws.com/test'


endpoint_url = 'http://ec2-54-80-175-58.compute-1.amazonaws.com:8082/{proxy}'

consumer = './kafka-console-consumer.sh --bootstrap-server b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098 --consumer.config client.properties --group students --topic 1272e2b5acdf.pin --from-beginning'

ec2_instance = 'ssh -i "1272e2b5acdf-key-pair.pem" ec2-user@ec2-54-80-175-58.compute-1.amazonaws.com'