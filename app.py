from flask import Flask, request, jsonify
from pymongo import MongoClient
from datetime import datetime
import os
import json

app = Flask(__name__)

# MongoDB connection
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
client = MongoClient(MONGO_URI)
db = client['github_events']
collection = db['webhook_data']

@app.route('/webhook', methods=['POST'])
def github_webhook():
    try:
        # Get GitHub event type from headers
        github_event = request.headers.get('X-GitHub-Event')
        payload = request.get_json()
        
        if not payload:
            return jsonify({'error': 'No payload received'}), 400
        
        # Prepare document for MongoDB based on the schema
        webhook_doc = {
            'timestamp': datetime.utcnow(),
            'action': '',
            'author': '',
            'request_id': '',
            'from_branch': '',
            'to_branch': ''
        }
        
        # Handle PUSH events
        if github_event == 'push':
            webhook_doc.update({
                'action': 'PUSH',
                'author': payload.get('pusher', {}).get('name', ''),
                'request_id': payload.get('head_commit', {}).get('id', ''),
                'from_branch': payload.get('ref', '').replace('refs/heads/', ''),
                'to_branch': payload.get('ref', '').replace('refs/heads/', '')
            })
        
        # Handle PULL REQUEST events
        elif github_event == 'pull_request':
            pr_data = payload.get('pull_request', {})
            action = payload.get('action', '')
            
            if action == 'opened':
                webhook_doc.update({
                    'action': 'PULL_REQUEST',
                    'author': pr_data.get('user', {}).get('login', ''),
                    'request_id': str(pr_data.get('id', '')),
                    'from_branch': pr_data.get('head', {}).get('ref', ''),
                    'to_branch': pr_data.get('base', {}).get('ref', '')
                })
            elif action == 'closed' and pr_data.get('merged', False):
                webhook_doc.update({
                    'action': 'MERGE',
                    'author': pr_data.get('merged_by', {}).get('login', '') or pr_data.get('user', {}).get('login', ''),
                    'request_id': str(pr_data.get('id', '')),
                    'from_branch': pr_data.get('head', {}).get('ref', ''),
                    'to_branch': pr_data.get('base', {}).get('ref', '')
                })
        
        # Only store if we have a valid action
        if webhook_doc['action']:
            # Insert into MongoDB
            result = collection.insert_one(webhook_doc)
            print(f"Stored webhook data: {webhook_doc}")
            
            return jsonify({
                'status': 'success',
                'message': f'{github_event} event processed',
                'id': str(result.inserted_id)
            }), 200
        else:
            return jsonify({
                'status': 'ignored',
                'message': f'{github_event} event not handled'
            }), 200
            
    except Exception as e:
        print(f"Error processing webhook: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/events', methods=['GET'])
def get_events():
    try:
        # Get recent events for the UI
        events = list(collection.find().sort('timestamp', -1).limit(50))
        
        # Convert ObjectId and datetime to strings for JSON response
        for event in events:
            event['_id'] = str(event['_id'])
            event['timestamp'] = event['timestamp'].strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        return jsonify(events), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat()
    }), 200

@app.route('/', methods=['GET'])
def index():
    return jsonify({
        'message': 'GitHub Webhook Receiver',
        'endpoints': {
            'webhook': '/webhook (POST)',
            'events': '/events (GET)',
            'health': '/health (GET)'
        }
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)