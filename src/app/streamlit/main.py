import streamlit as st
import time 
import os

# show mastodon key word list
mastodon_key_word_list = os.environ["MASTODON_KEY_WORD_LIST"]

# npl module
st.set_page_config(
    page_title="Real-Time Data Science Dashboard",
    page_icon="✅",
    layout="wide",
    menu_items={
        'Get Help': 'https://www.extremelycoolapp.com/help',
        'Report a bug': "https://www.extremelycoolapp.com/bug",
        'About': "# This is a header. This is an *extremely* cool app!"
    }
)

# Initialize connection.
conn = st.connection("postgresql", type="sql")



placeholder = st.empty()

# while or for statement
# near real-time / live feed simulation
# while True:
for seconds in range(600):

    # Perform query.
    # gruop by timestamp and prom polarity
    df = conn.query('select * from (SELECT * FROM stream where created is not null ORDER BY created DESC LIMIT 10) A order by created asc ;', ttl="1s") # ttl for caching

    with placeholder.container():

        st.markdown("### Mastodon NPL Analisys Pipeline - key word list: " + mastodon_key_word_list)

        #st.line_chart(data=None, *, x=None, y=None, color=None, width=0, height=0, use_container_width=True)
        st.line_chart(data=df, x='created', y='polarity')

        st.markdown("### Detailed Data View!!")

        df1 = df.sort_values(by='created', )

        # sort desc
        st.dataframe(df1, width=1500)
        time.sleep(1)