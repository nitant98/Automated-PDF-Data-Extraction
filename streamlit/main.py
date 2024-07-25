import streamlit as st
from streamlit_extras.switch_page_button import switch_page 
import app
import fetch_result

def main():
    st.sidebar.title("Navigation")
    selected_page = st.sidebar.radio("Go to", ["Home", "Fetch Results"])

    if selected_page == "Home":
        app.main()
    elif selected_page == "Fetch Results":
        fetch_result.main()

if __name__ == "__main__":
    main()