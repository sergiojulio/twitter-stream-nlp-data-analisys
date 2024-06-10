import streamlit as st
import time 
import os

# show mastodon key word list
mastodon_key_word_list = os.environ["MASTODON_KEY_WORD_LIST"]
#
postgres_db = os.environ["POSTGRES_DB"]
postgres_user = os.environ["POSTGRES_USER"]
postgres_pass = os.environ["POSTGRES_PASS"]
postgres_server = os.environ["POSTGRES_SERVER"]



st.set_page_config(
    page_title="Streaming npl data analysis",
    page_icon="✅",
    layout="wide",
    menu_items={
        'About': "https://github.com/sergiojulio"
    }
)

# Initialize connection.
conn = st.connection(
        "local_db", 
        type="sql",
        url="postgresql://" + postgres_user + ":" + postgres_pass + "@" + postgres_server + "/" + postgres_db
    )

placeholder = st.empty()

# near real-time / live feed simulation
# while True:
for seconds in range(600):

    df = conn.query('SELECT * FROM (SELECT * FROM stream WHERE created IS NOT NULL ORDER BY created DESC LIMIT 10) A ORDER BY created ASC;', ttl="1s") # ttl for caching

    with placeholder.container():

        st.markdown("### Mastodon NPL Analisys Pipeline - key word list: " + mastodon_key_word_list)
        #st.line_chart(data=None, *, x=None, y=None, color=None, width=0, height=0, use_container_width=True)
        st.line_chart(data=df, x='created', y='polarity')
        st.markdown("### Detailed Data View")
        df1 = df.sort_values(by='created', ascending=False)
        # sort desc
        st.dataframe(df1, width=1500)
        time.sleep(1)