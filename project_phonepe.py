import streamlit as st
from streamlit_option_menu import option_menu


st.set_page_config(layout='wide')
st.title('Phonepe Data Visualization And Exploration')

with st.sidebar:
    select = option_menu('Main Menu', ['Home', 'Data Exploration', 'Graphical representation'])

if select == 'Home':
    pass
elif select == 'Data Exploration':
    tab1, tab2, tab3 = st.columns(3)
    
    with tab1:
        st.write("##### Aggregated Results")
        option1 = st.radio('Choose the below options', ['Aggregated Transaction', 'Aggregated User'])
    
    with tab2:
        st.write("##### Map Results")
        option2 = st.radio('Choose the below options', ['Map Transaction', 'Map User'])
    
    with tab3:
        st.write("##### Top Results")
        option3 = st.radio('Choose the below options', ['Top Transaction', 'Top User'])
