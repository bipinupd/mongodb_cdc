from pymongo import MongoClient
import ast
import argparse
from google.cloud import pubsub_v1
from pathlib import Path
from bson import json_util


class MongodbCDC:

    def __init__(self, host, port, collection, project_id, topic_id,
                 state_threshold, state_file, resume_token):
        self.client = MongoClient(host, port)
        db = self.client.streams
        self.cdc_changes = db[collection]
        self.resume_token = resume_token
        self.state_threshold = state_threshold
        self.state_file = state_file
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_id)

    def write_state(self, change):
        with open(self.state_file, 'w') as f:
            f.write(json_util.dumps(change['_id']))

    def change_cdc(self):
        changes = 0
        with self.client.watch(resume_after=self.resume_token) as stream:
            for change in stream:
                changes = changes + 1
                data = json_util.dumps(change)
                data = data.encode("utf-8")
                self.publisher.publish(self.topic_path, data)
                if (changes >= self.state_threshold):
                    self.write_state(change)
                    changes = 0


def arguments():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument('--project_id',
                        required=True,
                        help='Project_id for PubSub Topic')
    parser.add_argument('--topic', required=True, help='Topic name')
    parser.add_argument('--mongodb_host', required=True, help='mongodb host')
    parser.add_argument('--port',
                        required=True,
                        type=int,
                        help='Port for mongodb host port to connect')
    parser.add_argument('--collection', required=True, help='collection')
    parser.add_argument('--threshold',
                        required=True,
                        type=int,
                        help='Threshold before writing state to the disk')
    args = parser.parse_args()
    return args


def main():
    args = arguments()
    _token = None
    source_path = Path(__file__).resolve()
    source_dir = source_path.parent
    file_name = 'state_' + args.collection + "_cdc"
    Path(source_dir / file_name).touch(exist_ok=True)
    with open(source_dir / file_name, 'r') as f:
        lines = f.read().splitlines()
        if (len(lines) > 0):
            last_line = lines[-1]
            _token = ast.literal_eval(last_line)
    MongodbCDC(args.mongodb_host, args.port, args.collection, args.project_id,
               args.topic, args.threshold, source_dir / file_name,
               _token).change_cdc()


if __name__ == '__main__':
    main()
