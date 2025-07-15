import streamlit as st
import json
import os
import time

BASE_DIR = os.path.dirname(__file__)
SPACES_CONFIG_FILE = os.path.join(BASE_DIR, "genie_spaces.json")
ACCESS_CONTROL_FILE = os.path.join(BASE_DIR, "genie_access_control.json")
ACCESS_REQUESTS_FILE = os.path.join(BASE_DIR, "genie_access_requests.json")
ADMIN_EMAIL = "aryan.bhagat@coindcx.com"

def load_spaces_config():
    with open(SPACES_CONFIG_FILE, 'r') as f:
        return json.load(f)

def load_access_control():
    if not os.path.exists(ACCESS_CONTROL_FILE):
        with open(ACCESS_CONTROL_FILE, 'w') as f:
            json.dump({}, f)
    with open(ACCESS_CONTROL_FILE, 'r') as f:
        return json.load(f)

def save_access_control(data):
    with open(ACCESS_CONTROL_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def load_access_requests():
    if not os.path.exists(ACCESS_REQUESTS_FILE):
        with open(ACCESS_REQUESTS_FILE, 'w') as f:
            json.dump([], f)
    with open(ACCESS_REQUESTS_FILE, 'r') as f:
        return json.load(f)

def save_access_requests(data):
    with open(ACCESS_REQUESTS_FILE, 'w') as f:
        json.dump(data, f, indent=2)

st.set_page_config(page_title="Genie Access Admin", layout="centered")

st.title("🔑 Genie Access Admin Panel")

# Only allow admin
if not (hasattr(st.user, "email") and st.user.email.lower() == ADMIN_EMAIL):
    st.error("Access restricted: Only admin can access this page.")
    st.stop()

spaces = load_spaces_config()
space_dict = {s['id']: s['name'] for s in spaces}

access_control = load_access_control()
requests_list = load_access_requests()

st.header("Pending Access Requests")
if not requests_list:
    st.info("No pending access requests.")
else:
    for idx, req in enumerate(requests_list):
        email = req['email']
        requested = req['requested_spaces']
        ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(req['timestamp']))
        st.markdown(f"**{email}** requested access on {ts}")
        st.markdown("Requested spaces:")
        st.write([space_dict.get(sid, sid) for sid in requested])
        # Grant access form
        with st.form(f"grant_form_{idx}"):
            grant_spaces = st.multiselect(
                "Grant access to:",
                options=requested,
                default=requested,
                format_func=lambda i: space_dict.get(i, i)
            )
            approve = st.form_submit_button("Grant Access")
        if approve and grant_spaces:
            # Update access control
            user_access = set(access_control.get(email, []))
            user_access.update(grant_spaces)
            access_control[email] = list(user_access)
            save_access_control(access_control)
            # Remove request
            requests_list = [r for r in requests_list if r['email'] != email]
            save_access_requests(requests_list)
            st.success(f"Granted access to {email} for: {[space_dict.get(sid, sid) for sid in grant_spaces]}")
            st.experimental_rerun()

st.header("Current User Access")
for email, space_ids in access_control.items():
    st.markdown(f"**{email}**: {[space_dict.get(sid, sid) for sid in space_ids]}") 