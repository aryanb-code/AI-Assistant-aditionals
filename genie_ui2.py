import streamlit as st
import requests
import time
import json
import pandas as pd
import sqlparse
import os
from typing import List, Dict

# ----------------- CONFIG -----------------
DATABRICKS_INSTANCE = "https://coindcx-dev.cloud.databricks.com"
BASE_DIR = os.path.dirname(__file__)
SPACES_CONFIG_FILE = os.path.join(BASE_DIR, "genie_spaces.json")
CHAT_HISTORY_FILE = os.path.join(BASE_DIR, "genie_chat_history.json")

# ----------------- DATABRICKS PAT AUTH -----------------
def get_pat_token():
    """Get PAT token from file"""
    pat_path = os.path.join(BASE_DIR, 'databricks_pat.txt')
    if os.path.exists(pat_path):
        with open(pat_path, 'r') as f:
            token = f.readline().strip()
        if token:
            return f"Bearer {token}"
    st.error("Databricks PAT not found. Please put your token in databricks_pat.txt.")
    st.stop()

# ----------------- GENIE SPACES CONFIG -----------------
def load_spaces_config() -> List[Dict]:
    if not os.path.exists(SPACES_CONFIG_FILE):
        with open(SPACES_CONFIG_FILE, 'w') as f:
            json.dump([{"id": "01f04a8868fa18fea33e66d898a1aa9b", "name": "Default Space"}], f, indent=2)
    with open(SPACES_CONFIG_FILE, 'r') as f:
        return json.load(f)

def save_spaces_config(spaces: List[Dict]):
    with open(SPACES_CONFIG_FILE, 'w') as f:
        json.dump(spaces, f, indent=2)

# ----------------- CHAT HISTORY -----------------
def load_chat_history() -> List[Dict]:
    if not os.path.exists(CHAT_HISTORY_FILE):
        with open(CHAT_HISTORY_FILE, 'w') as f:
            json.dump([], f)
    with open(CHAT_HISTORY_FILE, 'r') as f:
        return json.load(f)

def append_chat_history(entry: Dict):
    history = load_chat_history()
    history.append(entry)
    with open(CHAT_HISTORY_FILE, 'w') as f:
        json.dump(history, f, indent=2)

# ----------------- GENIE API FUNCTIONS -----------------
def start_conversation(prompt, space_id, auth_token):
    url = f"{DATABRICKS_INSTANCE}/api/2.0/genie/spaces/{space_id}/start-conversation"
    headers = {
        "Authorization": auth_token,
        "Content-Type": "application/json"
    }
    payload = {"content": prompt}
    res = requests.post(url, headers=headers, json=payload)
    
    if res.status_code == 200:
        return res.json()
    else:
        st.error(f"Failed to start conversation: {res.status_code} {res.text}")
        return None

def poll_genie_message(space_id, conversation_id, message_id, auth_token, poll_interval=2, timeout=300):
    url = f"{DATABRICKS_INSTANCE}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"
    headers = {"Authorization": auth_token}
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Polling failed: {response.status_code} - {response.text}")
        
        message = response.json()
        status = message.get("status")
        
        if status in ("COMPLETED", "FAILED"):
            return message
        
        time.sleep(poll_interval)
    
    raise TimeoutError("Message did not complete in time.")

def fetch_attachment(space_id, conversation_id, message_id, attachment_id, auth_token):
    url = f"{DATABRICKS_INSTANCE}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result/{attachment_id}"
    headers = {"Authorization": auth_token}
    res = requests.get(url, headers=headers)
    
    if res.status_code == 200:
        return res.json()
    else:
        st.error("Failed to fetch attachment result")
        return None

def extract_dataframe_from_genie_result(genie_result: dict) -> pd.DataFrame:
    try:
        statement = genie_result["statement_response"]
        columns = [col["name"] for col in statement["manifest"]["schema"]["columns"]]
        data = statement["result"]["data_array"]
        df = pd.DataFrame(data, columns=columns)
        return df
    except Exception as e:
        st.error(f"Failed to parse Genie result: {e}")
        return pd.DataFrame()

def send_followup_message(space_id, conversation_id, content, auth_token, host):
    url = f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages"
    headers = {
        "Authorization": auth_token,
        "Content-Type": "application/json"
    }
    payload = {"content": content}
    res = requests.post(url, headers=headers, json=payload)
    
    if res.status_code == 200:
        return res.json()
    else:
        st.error(f"Follow-up failed: {res.status_code} {res.text}")
        return None

def display_genie_message(message, space_id, token):
    st.subheader("🧠 Genie says:")
    st.markdown(f"**Prompt:** {message.get('content')}")
    
    attachments = message.get("attachments", [])
    if not attachments:
        st.info("No attachments found in the response.")
        return
    
    for attachment in attachments:
        if "text" in attachment:
            text = attachment["text"]["content"]
            st.markdown("**📋 Text Response:**")
            st.markdown(text)
            
        elif "query" in attachment:
            query_info = attachment["query"]
            with st.expander("📄 Show SQL Query Used", expanded=False):
                formatted_sql = sqlparse.format(query_info["query"], reindent=True, keyword_case='lower')
                st.markdown("**🧾 Query Description:**")
                st.info(query_info.get("description", ""))
                st.markdown("**📄 SQL Used:**")
                st.code(formatted_sql, language="sql")
            
            attachment_id = attachment.get("attachment_id")
            message_id = message["id"]
            conversation_id = message["conversation_id"]
            
            result_json = fetch_attachment(space_id, conversation_id, message_id, attachment_id, token)
            df = extract_dataframe_from_genie_result(result_json)
            
            if not df.empty:
                st.markdown("**📊 Query Result:**")
                st.dataframe(df)

# ----------------- STREAMLIT APP -----------------
st.set_page_config(page_title="Databricks Genie UI", layout="centered")

# Custom user info in top right (theme-aware, non-overlapping)
user_email = st.user.email if hasattr(st.user, "email") else "Unknown"
user_icon = "👤"  # You can use any emoji or even an image

st.markdown(
    f"""
    <style>
    .genie-user-pill {{
        position: fixed;
        top: 3.2rem;
        right: 1.2rem;
        z-index: 9999;
        background: var(--secondary-background-color, #f0f2f6);
        color: var(--text-color, #262730);
        padding: 0.18rem 0.7rem;
        border-radius: 1.2rem;
        display: flex;
        align-items: center;
        font-size: 0.98rem;
        border: 1px solid var(--primary-color, #e5e5e5);
        opacity: 0.97;
        box-shadow: none;
        transition: background 0.2s, color 0.2s;
    }}
    .genie-user-pill .icon {{
        font-size: 1.1rem;
        margin-right: 0.4rem;
    }}
    </style>
    <div class="genie-user-pill">
        <span class="icon">{user_icon}</span>
        <span>{user_email}</span>
    </div>
    """,
    unsafe_allow_html=True
)

# Google OIDC Login Check
if not st.user.is_logged_in:
    st.title("🔐 Databricks Genie Chat")
    st.markdown("### Please log in with your Google account to access the app")
    
    if st.button("🔑 Log in with Google", type="primary"):
        st.login()
    st.stop()

# Restrict access to @coindcx.com emails only
if not (hasattr(st.user, "email") and st.user.email.lower().endswith("@coindcx.com")):
    st.error("Access restricted: Only @coindcx.com email addresses are allowed.")
    st.stop()

# Main app (only shown after login)
st.title("🤖 Databricks Genie Chat")

# Welcome message with user info
st.markdown(f"### Welcome, **{st.user.name}**! 👋")
# Remove the old email display from the main page
# st.markdown(f"Email: {st.user.email}")

# Logout button in sidebar
# with st.sidebar:
#     st.markdown("---")
#     if st.button("🚪 Log out"):
#         st.logout()

# Get Databricks token
token = get_pat_token()

# Load spaces
spaces = load_spaces_config()
space_names = [s['name'] for s in spaces]
space_ids = [s['id'] for s in spaces]

# Sidebar: Select Genie Space, Chat History, Logout at bottom
with st.sidebar:
    st.markdown("### Select Genie Space:")
    if 'selected_space_idx' not in st.session_state:
        st.session_state['selected_space_idx'] = 0
    selected_space_idx = st.selectbox(
        "",
        range(len(space_names)),
        format_func=lambda i: space_names[i],
        index=st.session_state['selected_space_idx'],
        key="sidebar_space_selectbox"
    )
    st.session_state['selected_space_idx'] = selected_space_idx
    SPACE_ID = space_ids[selected_space_idx]

    st.markdown("---")
    st.title("🕑 Chat History")
    chat_history = load_chat_history()
    if chat_history:
        for i, entry in enumerate(reversed(chat_history)):
            if st.button(f"{entry['prompt'][:40]}...", key=f"hist_{i}"):
                st.session_state['conversation_id'] = entry['conversation_id']
                st.session_state['message_id'] = entry['message_id']
                st.session_state['last_prompt'] = entry['prompt']
    else:
        st.info("No chat history yet.")

    # Absolutely position logout button at the complete bottom of the sidebar
    st.markdown("""
        <style>
        div[data-testid="stSidebar"] > div:first-child {
            position: relative;
            min-height: 100vh;
        }
        .genie-logout-absolute-bottom {
            position: absolute;
            left: 0;
            right: 0;
            bottom: 1.5rem;
            width: 100%;
            padding: 0 1.2em;
        }
        </style>
    """, unsafe_allow_html=True)
    st.markdown('<div class="genie-logout-absolute-bottom">', unsafe_allow_html=True)
    if st.button("🚪 Log out"):
        st.logout()
    st.markdown('</div>', unsafe_allow_html=True)

# Main chat interface
with st.form("main_prompt_form", clear_on_submit=True):
    prompt = st.text_input(
        "Ask Genie something:", 
        placeholder="e.g., What is the volume in web3?", 
        key="main_prompt"
    )
    submitted = st.form_submit_button("Send to Genie")

if submitted and prompt:
    with st.spinner("Starting conversation..."):
        conv = start_conversation(prompt, SPACE_ID, token)
        
        if conv:
            conversation_id = conv["conversation_id"]
            message_id = conv["message_id"]
            
            st.session_state['conversation_id'] = conversation_id
            st.session_state['message_id'] = message_id
            st.session_state['last_prompt'] = prompt
            
            append_chat_history({
                "prompt": prompt,
                "conversation_id": conversation_id,
                "message_id": message_id,
                "space_id": SPACE_ID,
                "timestamp": time.time(),
                "user": st.user.email  # Track which user made the query
            })
            
            with st.spinner("Waiting for Genie response..."):
                msg = poll_genie_message(SPACE_ID, conversation_id, message_id, token)
                if msg:
                    display_genie_message(msg, SPACE_ID, token)

# Follow-up interface
if 'conversation_id' in st.session_state and 'message_id' in st.session_state:
    conversation_id = st.session_state['conversation_id']
    message_id = st.session_state['message_id']
    
    st.markdown("### 💬 Ask a follow-up question")
    
    with st.form("followup_form", clear_on_submit=True):
        follow_up = st.text_input(
            "Follow-up:", 
            placeholder="e.g. Which of these customers opened and forwarded the email?", 
            key="followup_prompt"
        )
        followup_submitted = st.form_submit_button("Send Follow-up")
        
        if followup_submitted and follow_up:
            response = send_followup_message(
                SPACE_ID,
                conversation_id,
                follow_up,
                token,
                DATABRICKS_INSTANCE
            )
            
            if response:
                message_id = response["message_id"]
                st.session_state['message_id'] = message_id
                
                append_chat_history({
                    "prompt": follow_up,
                    "conversation_id": conversation_id,
                    "message_id": message_id,
                    "space_id": SPACE_ID,
                    "timestamp": time.time(),
                    "user": st.user.email
                })
                
                with st.spinner("Polling Genie for follow-up response..."):
                    msg = poll_genie_message(SPACE_ID, conversation_id, message_id, token)
                    if msg:
                        display_genie_message(msg, SPACE_ID, token)