import streamlit as st
import time 


st.set_page_config(
    page_title="Real-Time Data Science Dashboard",
    page_icon="✅",
    layout="wide",
)

# Initialize connection.
conn = st.connection("postgresql", type="sql")



placeholder = st.empty()

# while or for statement
# near real-time / live feed simulation
# while True:
for seconds in range(200):

    # Perform query.
    df = conn.query('SELECT * FROM stream ORDER BY tweet_created ASC LIMIT 10;', ttl="1s") # ttl for caching

    with placeholder.container():
        #st.line_chart(data=None, *, x=None, y=None, color=None, width=0, height=0, use_container_width=True)
        st.line_chart(data=df, x='tweet_created', y='polarity')

        st.markdown("### Detailed Data View")
        st.dataframe(df)
        time.sleep(1)