import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector
import pandas as pd
import plotly.express as px
import locale as l
import json

class PhonepeDataVisualization:
    def __init__(self):
        self.mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database='phonephe'
        )
        self.mycursor = self.mydb.cursor(buffered=True) 

    def transaction_type(self,year,quarter,ways,state=None):
        if ways=='All India':
            query_oa_tran_type='SELECT sum(transaction_count) Tran ,transaction_type FROM aggregate_transaction where Quater=%s and year=%s group by transaction_type'
            self.mycursor.execute(query_oa_tran_type, (quarter, year))
            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_oa_trantype = pd.DataFrame(result, columns=columns)
            return df_oa_trantype
        elif ways=='State Wise':
            query_sw_tran_type='SELECT sum(transaction_count) Tran ,transaction_type FROM aggregate_transaction where Quater=%s and year=%s and state=%s group by transaction_type;'
            self.mycursor.execute(query_sw_tran_type, (quarter, year,state))
            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_sw_trantype = pd.DataFrame(result, columns=columns)
            return df_sw_trantype
        
    def top_transaction (self,year,quarter,ways,state=None):
        if ways=='All India':
            #top states
            query_oa_top_state='SELECT sum(transaction_count) "Transactions",state "States" FROM aggregate_transaction where quater=%s and year=%s group by state order by Transactions DESC limit 10'
            self.mycursor.execute(query_oa_top_state,(quarter,year))
            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_oa_top_state=pd.DataFrame(result,columns=columns)
            
            #top district
            query_oa_top_dist='SELECT sum(transaction_count) "Transactions" ,district "Districts" FROM map_tran where quater=%s and year=%s group by district order by Transactions DESC limit 10'
            self.mycursor.execute(query_oa_top_dist,(quarter,year))
            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_oa_top_dist=pd.DataFrame(result,columns=columns)

            #top pincode
            query_oa_top_pin='''SELECT sum(count) "Transactions" ,pincode "Pincodes" 
                              FROM top_tran where quater=%s and year=%s 
                              group by pincode 
                              order by Transactions DESC 
                              limit 10'''
            self.mycursor.execute(query_oa_top_pin,(quarter,year))
            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_oa_top_pin=pd.DataFrame(result,columns=columns)
            return df_oa_top_state,df_oa_top_dist,df_oa_top_pin
        elif ways=='State Wise':
            #top district
            query_sw_top_dist='''SELECT sum(transaction_count) "Transactions" ,district "Districts" 
                                 FROM map_tran where quater=%s and year=%s and state=%s 
                                 group by district order by Transactions DESC limit 10'''
            self.mycursor.execute(query_sw_top_dist,(quarter,year,state))
            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_sw_top_dist=pd.DataFrame(result,columns=columns)

            #top pincode
            query_sw_top_pin='''select sum(count) "Transactions" ,pincode "Pincodes" 
                              from top_tran 
                              where 
                              quater=%s and                                                            
                              year=%s and 
                              state=%s 
                              group by pincode 
                              order by amount DESC limit 10'''
            self.mycursor.execute(query_sw_top_pin,(quarter,year,state))
            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_sw_top_pin=pd.DataFrame(result,columns=columns)
            return df_sw_top_dist,df_sw_top_pin
            


        
    def Agggregate_Tran_Section(self, year, quarter,ways,state=None):
        
        if ways=='All India':

            # Overall Transaction
            query_oa_year_quater = 'SELECT state "States", transaction_count "Transaction Count", transaction_amount "Transaction Amount", round(transaction_amount/transaction_count,2) "Avg Transaction" FROM aggregate_transaction WHERE quater=%s AND year=%s GROUP BY state'
            self.mycursor.execute(query_oa_year_quater, (quarter, year))

            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_aggregate_tran_year_quater = pd.DataFrame(result, columns=columns)


            # Amount,Transaction,Avg
            query_oa_year_quater = 'SELECT sum(transaction_amount) amt, sum(transaction_count) tran, (sum(transaction_amount)/sum(transaction_count)) avg  FROM aggregate_transaction WHERE quater=%s AND year=%s'
            self.mycursor.execute(query_oa_year_quater, (quarter, year))
            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_overall = pd.DataFrame(result, columns=columns)
            total_amount = df_overall['amt'].iloc[0]
            total_tran = df_overall['tran'].iloc[0]
            total_avg = df_overall['avg'].iloc[0]


            fig_geo = px.choropleth(
                    df_aggregate_tran_year_quater,
                    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                    featureidkey='properties.ST_NM',
                    locations='States', 
                    color='Transaction Amount',
                    color_continuous_scale='Reds',
                    hover_name='States',
                    hover_data={'Transaction Amount', 'Transaction Count', 'Avg Transaction'},
                    width=700,
                    height=500,
                    title=f'PhonePe Pulse in {year} for Quarter Q{quarter}'
                )

            fig_geo.update_geos(fitbounds="locations", visible=False)
            fig_geo.update_traces(hovertemplate='<b>%{hovertext}</b><br><b>Transaction Count:</b> %{customdata[1]:}<br><b>Transaction Amount:</b> ₹%{customdata[0]:.2f}<br><b>Transaction Amount:</b> ₹%{customdata[2]}<extra></extra>')
            st.plotly_chart(fig_geo)

            # elif option_type == 'All India':

            st.write('##### Transactions')
            l.setlocale(l.LC_ALL, 'en_IN.UTF-8')
            total_tran = l.format_string("%d", total_tran, grouping=True)
            st.metric(label="All PhonePe transactions (UPI + Cards + Wallets)", value=total_tran)
            l.setlocale(l.LC_MONETARY, 'en_IN')
            st.metric(label="Total payment value", value=l.currency(total_amount, grouping=True))
            st.metric(label="Avg. transaction value", value=l.currency(total_avg, grouping=True))
        
        elif  ways=='State Wise':



            # StateWise Transaction
            query_sw_year_quater = '''SELECT transaction_count "Transaction Count", round(transaction_amount,2) "Transaction Amount", round(transaction_amount/transaction_count,2) "Avg Transaction",district "Districts"
                                      FROM map_tran WHERE quater=%s AND year=%s and state=%s
                                      GROUP BY district'''
            
            self.mycursor.execute(query_sw_year_quater, (quarter, year,state))

            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_aggregate_tran_year_quater = pd.DataFrame(result, columns=columns)

        # Amount,Transaction,Avg
            query_oa_year_quater = '''SELECT  sum(transaction_count) tran, sum(transaction_amount) amt, round(sum(transaction_amount)/sum(transaction_count),2) avg
                                    FROM aggregate_transaction 
                                    WHERE quater=%s AND year=%s and state=%s GROUP BY state'''
            
            self.mycursor.execute(query_oa_year_quater, (quarter, year,state))
            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_overall = pd.DataFrame(result, columns=columns)
            total_amount = df_overall['amt'].iloc[0]
            total_tran = df_overall['tran'].iloc[0]
            total_avg = df_overall['avg'].iloc[0]            

            # District wise chart
            fig = px.bar(df_aggregate_tran_year_quater, x='Districts', y='Transaction Count',
                          hover_data={'Transaction Count','Transaction Amount','Avg Transaction'},
                            color_discrete_sequence=['red'])
            st.plotly_chart(fig)

            # elif option_type == 'All India':
            # l.setlocale(l.LC_MONETARY, 'en_IN')
            # st.write('##### Transactions')
            # st.metric(label="All PhonePe transactions (UPI + Cards + Wallets)", value=l.currency(total_tran, grouping=True, symbol=False))
            # st.metric(label="Total payment value", value=l.currency(total_amount, grouping=True))
            # st.metric(label="Avg. transaction value", value=l.currency(total_avg, grouping=True))

            st.write('##### Transactions')
            l.setlocale(l.LC_ALL, 'en_IN.UTF-8')
            total_tran = l.format_string("%d", total_tran, grouping=True)
            st.metric(label="All PhonePe transactions (UPI + Cards + Wallets)", value=total_tran)
            l.setlocale(l.LC_MONETARY, 'en_IN')
            st.metric(label="Total payment value", value=l.currency(total_amount, grouping=True))
            st.metric(label="Avg. transaction value", value=l.currency(total_avg, grouping=True))


    def Aggregate_User_section (self,year,quarter,ways,state=None):
        if ways=='All India':

            # Overall user 
            query_oa_year_quater = '''SELECT  SUM(Registered_Users) AS "Registered User",
                                    sum(App_Opens) "Apps Opened"
                                    FROM map_user 
                                    WHERE quater = %s AND year = %s'''
            self.mycursor.execute(query_oa_year_quater, (quarter, year))
            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_aggregate_user_year_quater = pd.DataFrame(result, columns=columns)
            # tot_reg=df_aggregate_user_year_quater['Registered User']

            query_oa_plot = '''SELECT  state as "States",SUM(Registered_Users) AS "Registered User",
                                    sum(App_Opens) "Apps Opened"
                                    FROM map_user 
                                    WHERE quater = %s AND year = %s group by state'''
            self.mycursor.execute(query_oa_plot, (quarter, year))
            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_aggregate_user_plot = pd.DataFrame(result, columns=columns)


            fig_geo = px.choropleth(
                    df_aggregate_user_plot,
                    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                    featureidkey='properties.ST_NM',
                    locations='States', 
                    color='Registered User',
                    color_continuous_scale='Reds',
                    hover_name='States',
                    hover_data={'Registered User', 'Apps Opened'},
                    width=700,
                    height=500,
                    title=f'Registered PhonePe Users in {year} for Quarter Q{quarter}'
                )

            fig_geo.update_geos(fitbounds="locations", visible=False)

            fig_geo.update_traces(hovertemplate='<b>%{hovertext}</b><br><b>Registered User:</b> %{customdata[1]:}<br><b>Apps Opened:</b> %{customdata[0]:}<br><extra></extra>')


            st.plotly_chart(fig_geo)

            # Extract the total registered users
            if df_aggregate_user_year_quater.empty or df_aggregate_user_year_quater['Registered User'].isnull().all():
                tot_reg = 0
            else:
                tot_reg = int(df_aggregate_user_year_quater.at[0, 'Registered User'])

            if df_aggregate_user_year_quater.empty or df_aggregate_user_year_quater['Apps Opened'].isnull().all():
                tot_app = 'Not Available'
            else:
                tot_app = df_aggregate_user_year_quater.at[0, 'Apps Opened']
            if tot_app == 0:
                tot_app = 'Not Available'
                tot_app=int(df_aggregate_user_year_quater['Apps Opened'])



            l.setlocale(l.LC_ALL, 'en_IN.UTF-8')
            tot_reg = l.format_string("%d", tot_reg, grouping=True)

            if tot_app!='Not Available':
                tot_app = l.format_string("%d", int(tot_app), grouping=True)


            st.write('##### Registered User')
            st.metric(label="Registered Users", value= tot_reg)
            st.metric(label="Apps Opened", value=tot_app)

        elif ways=='State Wise':
            # StateWise users
            query_sw_year_quater = '''SELECT District "Districts",sum(Registered_Users) "Registered Users",sum(App_Opens) "Apps Opened" 
                                      FROM map_user 
                                      where Quater=%s and year=%s and state=%s
                                      group by District'''
            
            self.mycursor.execute(query_sw_year_quater, (quarter, year,state))

            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_aggregate_user_year_quater = pd.DataFrame(result, columns=columns)

            query_sw_tot = '''SELECT sum(Registered_Users) "Registered Users",sum(App_Opens) "Apps Opened" 
                              FROM map_user 
                              WHERE Quater=%s and year=%s and state=%s'''
            
            self.mycursor.execute(query_sw_tot, (quarter, year,state))
            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_overall = pd.DataFrame(result, columns=columns)
            
            tot_reg = df_overall['Registered Users'].iloc[0]
            tot_app = df_overall['Apps Opened'].iloc[0]
        
         

            # District wise chart
            fig = px.bar(df_aggregate_user_year_quater, x='Districts', y='Registered Users',
                          hover_data={'Registered Users','Apps Opened'},
                            color_discrete_sequence=['red'])
            st.plotly_chart(fig)


            # Extract the total registered users
            if df_overall.empty or df_overall['Registered Users'].isnull().all():
                tot_reg = 0
            else:
                tot_reg = int(df_overall.at[0, 'Registered Users'])

            if df_overall.empty or df_overall['Apps Opened'].isnull().all():
                tot_app = 'Not Available'
            else:
                tot_app = df_overall.at[0, 'Apps Opened']
            if tot_app == 0:
                tot_app = 'Not Available'
                tot_app=int(df_overall['Apps Opened'])



            l.setlocale(l.LC_ALL, 'en_IN.UTF-8')
            tot_reg = l.format_string("%d", tot_reg, grouping=True)

            if tot_app!='Not Available':
                tot_app = l.format_string("%d", int(tot_app), grouping=True)



            st.write('##### Users')
            st.metric(label=f'Registered PhonePe Users till {year} Q{quarter}', value=tot_reg)
            st.metric(label=f'PhonePe App Opens in {year} Q{quarter}', value=tot_app)


            
           
    
    def top_users(self,year,quarter,ways,state=None):
        if ways=='All India':
            # top states 
            query_oa_top_state = '''select State "States",sum(Registered_Users) "Registered User" 
                                from map_user 
                                where quater=%s and year=%s 
                                group by state order by 2 DESC 
                                limit 10'''
            self.mycursor.execute(query_oa_top_state, (quarter, year))
            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_oa_top_state = pd.DataFrame(result, columns=columns)
            
            # top distrcts 
            query_oa_top_dist='''select District "Districts",sum(Registered_Users) "Registered User" 
                                 from map_user 
                                 where quater=%s and year=%s 
                                 group by District 
                                 order by 2 DESC 
                                 limit 10'''
            
            self.mycursor.execute(query_oa_top_dist, (quarter, year))
            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_oa_top_dist = pd.DataFrame(result, columns=columns)

            # top pincode 
            query_oa_top_pin='''select pincode "Pincodes",sum(Registered_User) "Registered User" 
                                 from top_user 
                                 where quater=%s and year=%s 
                                 group by pincode 
                                 order by 2 DESC 
                                 limit 10'''
            
            self.mycursor.execute(query_oa_top_pin, (quarter, year))
            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_oa_top_pin = pd.DataFrame(result, columns=columns) 
            return df_oa_top_state,df_oa_top_dist,df_oa_top_pin

        elif ways=='State Wise':
            # top distrcts 
            query_sw_top_dist='''select District "Districts",sum(Registered_Users) "Registered Users" 
                                 from map_user 
                                where quater=%s and year=%s and state=%s
                                group by District 
                                order by 2 DESC 
                                limit 10'''
            
            self.mycursor.execute(query_sw_top_dist, (quarter, year,state))
            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_sw_top_dist = pd.DataFrame(result, columns=columns)

            # top pincode 
            query_sw_top_pin='''select pincode "Pincodes",sum(Registered_User) "Registered Users" 
                                from top_user 
                                where quater=%s and year=%s and state=%s
                                group by pincode 
                                order by 2 DESC 
                                limit 10'''
            
            self.mycursor.execute(query_sw_top_pin, (quarter, year,state))
            result = self.mycursor.fetchall()
            columns = [i[0] for i in self.mycursor.description]
            df_sw_top_pin = pd.DataFrame(result, columns=columns) 
            return df_sw_top_dist,df_sw_top_pin

    

                    

                
# Streamlit part

if __name__ == "__main__":
    st.set_page_config(layout='wide')
    st.title('Phonepe Data Visualization And Exploration')


    with st.sidebar:
        select = option_menu('Main Menu', ['Home', 'Data Exploration', 'Graphical representation'])

    if select == 'Home':
        col1,col2=st.columns(2)

        with col1:
            pic='C:/Projects/vscode/phonepe/pic.webp'
            st.image(pic,caption="CEO & Founder of Phonepe: Mr. Sameer Nigam",use_column_width=True)

        with col2:
            st.write("##### The Evolution & Future of India's Digital Payments Industry")
            st.write("PhonePe Group is India’s leading fintech company. Its flagship product, the PhonePe digital payments app, was launched in Aug 2016. Within a short period of time, the company has scaled rapidly to become India’s leading consumer payments app. On the back of its leadership in digital payments, PhonePe Group has expanded into financial services - Insurance, Lending, & Wealth as well as new consumer tech businesses - Pincode and Indus Appstore.")

    
        
    elif select == 'Data Exploration':
        tab1, tab2 = st.columns(2)

        with tab1:
            option1 = st.selectbox('Choose the below options', ['Transactions', 'Users'])

            if option1 == 'Transactions':
                
                obj = PhonepeDataVisualization()

                obj.mycursor.execute('SELECT DISTINCT year FROM aggregate_transaction')
                years = [row[0] for row in obj.mycursor.fetchall()]

                inp_year = st.selectbox('Select Year', years)
                quarter = st.selectbox("Select Quarter", [1, 2, 3, 4])

                with st.popover("Geographical View", use_container_width=True):
                    
                    option_type = st.selectbox('selection options:', ['All India', 'State Wise'])
                    
                    if option_type=='All India':

                        obj.Agggregate_Tran_Section(inp_year, quarter,option_type)
                        
                        st.markdown('----------------------------------')
                        st.write('##### Categories')
                        df_tran_type = obj.transaction_type(inp_year, quarter, option_type)
                        
                        #transaction type chart
                        fig_pie=px.pie(values=df_tran_type['Tran'],names=df_tran_type['transaction_type'])
                        st.plotly_chart(fig_pie)
                        
                        df_state,df_dist,df_pin=obj.top_transaction(inp_year,quarter,option_type)
                        
                        st.markdown('-------------------------------------------')
                        
                        tabs = st.tabs(['States', 'Districts', 'Pincodes'])

                        # States tab
                        with tabs[0]:
                            st.header('Top 10 States')
                            opt = st.radio('Select options', ['State Charts', 'State Data'])
                            if opt == 'State Charts':
                                fig = px.bar(df_state, x='States', y='Transactions', hover_data='Transactions', color_discrete_sequence=['red'])
                                st.plotly_chart(fig)
                            else:
                                st.dataframe(df_state, hide_index=True)

                        # Districts tab
                        with tabs[1]:
                            st.header('Top 10 Districts')
                            opt = st.radio('Select options', ['District Charts', 'District Data'])
                            if opt == 'District Charts':
                                fig = px.bar(df_dist, x='Districts', y='Transactions', hover_data='Transactions', color_discrete_sequence=['red'])
                                st.plotly_chart(fig)
                            else:
                                st.dataframe(df_dist, hide_index=True)

                        # Pincodes tab
                        with tabs[2]:
                            st.header('Top 10 Postal Codes')
                            opt = st.radio('Select options', ['Pincode Charts', 'Pincode Data'])
                            if opt == 'Pincode Charts':
                                
                                df_pin_sorted = df_pin.sort_values(by='Transactions', ascending=False)
                                df_pin_sorted['Pincodes'] = df_pin_sorted['Pincodes'].astype(str)
                                pincode_order = df_pin_sorted['Pincodes'].tolist()
                                
                                # fig = px.bar(df_pin_sorted, x='Pincodes', y='Transactions', color_discrete_sequence=['red'])
                                # fig.update_xaxes(tickvals=pincode_order)

                                # # Update layout to improve readability
                                # fig.update_layout(xaxis={'title': 'Pincode'},
                                #                 yaxis={'title': 'Transaction Count'})
                                
                                # st.plotly_chart(fig)

                                fig_pie=px.pie(values=df_pin_sorted['Transactions'],names=df_pin_sorted['Pincodes'])
                                st.plotly_chart(fig_pie)


                            else:
                                df_pin['Pincodes']=df_pin['Pincodes'].astype(str)
                                st.dataframe(df_pin, hide_index=True)
                        
                    elif option_type=='State Wise':

                        obj.mycursor.execute('select distinct state from aggregate_transaction')
                        states = [row[0] for row in obj.mycursor.fetchall()]
                        st_inp=st.selectbox('States',states)
                        obj.Agggregate_Tran_Section(inp_year, quarter,option_type,st_inp)
                        
                        st.markdown('----------------------------------')
                        st.write('##### Categories')
                        df_tran_type = obj.transaction_type(inp_year, quarter, option_type,st_inp)
                        
                        #transaction type chart
                        fig_pie=px.pie(values=df_tran_type['Tran'],names=df_tran_type['transaction_type'])
                        st.plotly_chart(fig_pie)
                        
                        df_dist,df_pin=obj.top_transaction(inp_year,quarter,option_type,st_inp)
                        
                        st.markdown('-------------------------------------------')
                        tabs = st.tabs(['Districts', 'Pincodes'])

                        # Districts tab
                        with tabs[0]:
                            st.header('Top 10 Districts')
                            opt = st.radio('Select options', ['District Charts', 'District Data'])
                            if opt == 'District Charts':
                                fig = px.bar(df_dist, x='Districts', y='Transactions', hover_data='Transactions', color_discrete_sequence=['red'])
                                st.plotly_chart(fig)
                            else:
                                st.dataframe(df_dist, hide_index=True)

                        # Pincodes tab
                        with tabs[1]:
                            st.header('Top 10 Postal Codes')
                            opt = st.radio('Select options', ['Pincode Charts', 'Pincode Data'])
                            if opt == 'Pincode Charts':
                                
                                df_pin_sorted = df_pin.sort_values(by='Transactions', ascending=False)
                                df_pin_sorted['Pincodes'] = df_pin_sorted['Pincodes'].astype(str)
                                pincode_order = df_pin_sorted['Pincodes'].tolist()
                                # plot
                                fig_pie=px.pie(values=df_pin['Transactions'],names=df_pin['Pincodes'])
                                st.plotly_chart(fig_pie)


                            else:
                                df_pin['Pincodes']=df_pin['Pincodes'].astype(str)
                                st.dataframe(df_pin, hide_index=True)
            
            elif option1=='Users':
                obj = PhonepeDataVisualization()

                obj.mycursor.execute('SELECT DISTINCT year FROM map_user')
                years = [row[0] for row in obj.mycursor.fetchall()]

                inp_year = st.selectbox('Select Year', years)
                quarter = st.selectbox("Select Quarter", [1, 2, 3, 4])

                with st.popover("Geographical View", use_container_width=True):
                    
                    option_type = st.selectbox('selection options:', ['All India', 'State Wise'])
                    
                    if option_type=='All India':

                        obj.Aggregate_User_section(inp_year, quarter,option_type)  

                        st.markdown('-------------------------------------------')
                        
                        tabs = st.tabs(['States', 'Districts', 'Pincodes'])
                        
                        df_state,df_dist,df_pin=obj.top_users(inp_year,quarter,option_type)

                        # States tab
                        with tabs[0]:
                            st.header('Top 10 States')
                            opt = st.radio('Select options', ['State Charts', 'State Data'])
                            if opt == 'State Charts':
                                fig = px.bar(df_state, x='States', y='Registered User', hover_data='Registered User', color_discrete_sequence=['red'])
                                st.plotly_chart(fig)
                            else:
                                st.dataframe(df_state, hide_index=True)

                        # Districts tab
                        with tabs[1]:
                            st.header('Top 10 Districts')
                            opt = st.radio('Select options', ['District Charts', 'District Data'])
                            if opt == 'District Charts':
                                fig = px.bar(df_dist, x='Districts', y='Registered User', hover_data='Registered User', color_discrete_sequence=['red'])
                                st.plotly_chart(fig)
                            else:
                                st.dataframe(df_dist, hide_index=True)

                        # Pincodes tab
                        with tabs[2]:
                            st.header('Top 10 Postal Codes')
                            opt = st.radio('Select options', ['Pincode Charts', 'Pincode Data'])
                            if opt == 'Pincode Charts':
                                
                                df_pin_sorted = df_pin.sort_values(by='Registered User', ascending=False)
                                df_pin_sorted['Pincodes'] = df_pin_sorted['Pincodes'].astype(str)
                                pincode_order = df_pin_sorted['Pincodes'].tolist()
                                
                                fig_pie=px.pie(values=df_pin_sorted['Registered User'],names=df_pin_sorted['Pincodes'])
                                st.plotly_chart(fig_pie)


                            else:
                                df_pin['Pincodes']=df_pin['Pincodes'].astype(str)
                                st.dataframe(df_pin, hide_index=True)  

                    elif option_type=='State Wise':
                        
                        obj.mycursor.execute('select distinct state from map_user')
                        states = [row[0] for row in obj.mycursor.fetchall()]
                        st_inp=st.selectbox('States',states)   

                        obj.Aggregate_User_section(inp_year, quarter,option_type,st_inp)    

                        df_dist,df_pin=obj.top_users(inp_year,quarter,option_type,st_inp)
                        
                        st.markdown('-------------------------------------------')
                        tabs = st.tabs(['Districts', 'Pincodes'])

                        # Districts tab
                        with tabs[0]:
                            st.header('Top 10 Districts')
                            opt = st.radio('Select options', ['District Charts', 'District Data'])
                            if opt == 'District Charts':
                                fig = px.bar(df_dist, x='Districts', y='Registered Users', hover_data='Registered Users', color_discrete_sequence=['red'])
                                st.plotly_chart(fig)
                            else:
                                st.dataframe(df_dist, hide_index=True)

                        # Pincodes tab
                        with tabs[1]:
                            st.header('Top 10 Postal Codes')
                            opt = st.radio('Select options', ['Pincode Charts', 'Pincode Data'])
                            if opt == 'Pincode Charts':
                                
                                df_pin_sorted = df_pin.sort_values(by='Registered Users', ascending=False)
                                df_pin_sorted['Pincodes'] = df_pin_sorted['Pincodes'].astype(str)
                                pincode_order = df_pin_sorted['Pincodes'].tolist()
                                # plot
                                fig_pie=px.pie(values=df_pin['Registered Users'],names=df_pin['Pincodes'])
                                st.plotly_chart(fig_pie)


                            else:
                                df_pin['Pincodes']=df_pin['Pincodes'].astype(str)
                                st.dataframe(df_pin, hide_index=True)



                        


                    
                    

