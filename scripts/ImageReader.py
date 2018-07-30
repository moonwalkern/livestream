import cv2
from kafka import KafkaProducer
import base64
print(cv2.__version__)

vidcap = cv2.VideoCapture('/Users/sreeji/Documents/Sreeji/work/LiveStream/images_classification/mp4/big_buck_bunny_720p_5mb.mp4')
success,image = vidcap.read()
count = 0
success = True
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Asynchronous by default

# future = producer.send('streamer', b'sreeji gopal1')

# # Block for 'synchronous' sends
# try:
#     record_metadata = future.get(timeout=10)
# except KafkaError:
#     # Decide what to do if produce request failed...
#     log.exception()
#     pass

# Successful result returns assigned partition and offset
# print (record_metadata.topic)
# print (record_metadata.partition)
# print (record_metadata.offset)

while success:
    # cv2.imwrite("image/frame%d.jpg" % count, image)     # save frame as JPEG file

    success,image = vidcap.read()
    ret, jpeg = cv2.imencode('.png', image)
    print 'Read a new frame: ',  jpeg.tobytes()
    future = producer.send('streamer', "jpg")
    future = producer.send('streamer', jpeg)
    break