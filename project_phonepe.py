import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector
import pandas as pd
import plotly.express as px

        # Establish a connection to the MySQL database
mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database='phonephe'
        )
mycursor = mydb.cursor(buffered=True) 

def Agggregate_Section(year, quarter):
       
        Tran_query = 'SELECT state "States", transaction_count "Transaction Count", transaction_amount "Transaction Amount" FROM aggregate_transaction WHERE quater=%s AND year=%s GROUP BY state'
        mycursor.execute(Tran_query, (quarter, year))

        result = mycursor.fetchall()
        columns = [i[0] for i in mycursor.description]
        df_aggregate = pd.DataFrame(result, columns=columns)
        
        # Line plot for transaction count
        fig_count = px.line(df_aggregate, x='States', y='Transaction Count', title='Aggregated Transaction Count',color_discrete_sequence=['red'])
        fig_count.update_layout( title_x=0.4,width=700,height=500)
        st.plotly_chart(fig_count)
            
        # Line plot for transaction amount
        fig_amount = px.line(df_aggregate, x='States', y='Transaction Amount', title='Aggregated Transaction Amount',color_discrete_sequence=['purple'])
        fig_amount.update_layout( title_x=0.4,width=700,height=500)
        st.plotly_chart(fig_amount)

        # Geoplot
        # st.write("### Geographical Plot")
        fig_geo = px.choropleth(
                df_aggregate,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations='States',
                color='Transaction Amount',
                color_continuous_scale='purples',
                range_color=(df_aggregate['Transaction Amount'].min(),df_aggregate['Transaction Amount'].max()),
                hover_name='States',
                hover_data={'Transaction Amount'},
                width=700,
                height=500 ,
                title=f'Total Amount Transacted in {year} for Quarter Q{quarter}'  # Add Transaction Count to hover data
            )

        fig_geo.update_geos(fitbounds="locations", visible=False)
        # fig_geo.show()
        st.plotly_chart(fig_geo)

        fig_geo = px.choropleth(
                df_aggregate,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations='States',
                color='Transaction Count',
                color_continuous_scale='Reds',
                range_color=(df_aggregate['Transaction Count'].min(),df_aggregate['Transaction Count'].max()),
                hover_name='States',
                hover_data={'Transaction Count'},
                width=700,
                height=500 ,
                title=f'Total Transaction Count in {year} for Quarter Q{quarter}'  # Add Transaction Count to hover data
            )

        fig_geo.update_geos(fitbounds="locations", visible=False)
        # fig_geo.show()
        st.plotly_chart(fig_geo)



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
        option1 = st.selectbox('Choose the below options', ['Aggregated Transaction', 'Aggregated User'])
        
        if option1=='Aggregated Transaction':

            mycursor.execute('select distinct year from aggregate_transaction')
            years = [row[0] for row in mycursor.fetchall()]

            inp_year=st.selectbox ('Select Year',years)
            quarter= st.selectbox("Select Quarter", [1, 2, 3, 4])
            
            Agggregate_Section(inp_year,quarter)

    with tab2:
        st.write("##### Map Results")
        option2 = st.selectbox('Choose the below options', ['Map Transaction', 'Map User'])
    
    with tab3:
        st.write("##### Top Results")
        option3 = st.selectbox('Choose the below options', ['Top Transaction', 'Top User'])
