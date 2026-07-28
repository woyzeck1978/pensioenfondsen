import streamlit as st
import pandas as pd

if "page" not in st.session_state:
    st.session_state.page = "Overview"

pages = ["Overview", "Deep-Dive"]
st.sidebar.radio("Go to", pages, key="page")

if st.session_state.page == "Overview":
    st.write("Overview Page")
    df = pd.DataFrame({"name": ["Fund A", "Fund B", "Fund C"], "val": [1, 2, 3]})
    
    # Provide instruction
    st.write("Select a row below to dive into the fund details.")
    
    event = st.dataframe(
        df,
        on_select="rerun",
        selection_mode="single-row",
        key="overview_grid"
    )
    if event.selection.rows:
        selected_idx = event.selection.rows[0]
        st.session_state.selected_fund = df.iloc[selected_idx]["name"]
        st.session_state.page = "Deep-Dive"
        st.rerun()

elif st.session_state.page == "Deep-Dive":
    st.write("Deep-Dive Page")
    st.write(f"Selected Fund: {st.session_state.get('selected_fund', 'None')}")
