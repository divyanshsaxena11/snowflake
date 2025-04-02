# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import plotly.graph_objects as go
import plotly.express as px
# Write directly to the app
st.set_page_config(layout="wide")
st.title("Snowflake Iceberg Lakehouse for Indian Premier League - 2025 🏏")
st.write("Replace the code in this example app with your own code! And if you're new to Streamlit, here are some helpful links:")

# Get the current credentials
session = get_active_session()
st.header("Team squad Strength")
df3 = session.sql("SELECT TEAM_NAME, TEAM_IMAGE, COUNT(*) AS PLAYER_COUNT FROM CDW_LAKEHOUSE.GOLD.MATCHES_SQUADS_INFO GROUP BY TEAM_NAME, TEAM_IMAGE ORDER BY PLAYER_COUNT DESC").to_pandas()
for index, row in df3.iterrows():
    col1, col2, col3 = st.columns([1, 3, 2])
    with col1:
        st.image(row["TEAM_IMAGE"], width=50)
    with col2:
        st.write(f"**{row['TEAM_NAME']}**")
    with col3:
        st.write(f"Players: {row['PLAYER_COUNT']}")

st.header("Best Combination of Players Per Role")
df1 = session.sql("""
SELECT TEAM_NAME,
       SUM(CASE WHEN ROLE = 'Batsman' THEN 1 ELSE 0 END) AS BATSMEN,
       SUM(CASE WHEN ROLE = 'Bowler' THEN 1 ELSE 0 END) AS BOWLERS,
       SUM(CASE WHEN ROLE = 'All-Rounder' THEN 1 ELSE 0 END) AS ALL_ROUNDERS,
       SUM(CASE WHEN ROLE = 'Wicketkeeper' THEN 1 ELSE 0 END) AS WICKETKEEPERS
FROM CDW_LAKEHOUSE.GOLD.MATCHES_SQUADS_INFO
GROUP BY TEAM_NAME
""").to_pandas()
# Radar Chart
fig = go.Figure()
for _, row in df1.iterrows():
    fig.add_trace(go.Scatterpolar(
        r=[row["BATSMEN"], row["BOWLERS"], row["ALL_ROUNDERS"], row["WICKETKEEPERS"]],
        theta=['BATSMEN', 'BOWLERS', 'ALL_ROUNDERS', 'WICKETKEEPERS'],
        fill='toself',
        name=row["TEAM_NAME"]
    ))
fig.update_layout(polar=dict(radialaxis=dict(visible=True)), title="Best Team Combinations")
st.plotly_chart(fig)

df2_batting = session.sql("SELECT BATTING_STYLE, COUNT(*) COUNT FROM CDW_LAKEHOUSE.GOLD.MATCHES_SQUADS_INFO WHERE BATTING_STYLE IS NOT NULL GROUP BY BATTING_STYLE ORDER BY COUNT DESC").to_pandas()

fd2_bowling = session.sql("SELECT BOWLING_STYLE, COUNT(*) count FROM CDW_LAKEHOUSE.GOLD.MATCHES_SQUADS_INFO WHERE BOWLING_STYLE IS NOT NULL GROUP BY BOWLING_STYLE ORDER BY COUNT DESC").to_pandas()
col1, col2 = st.columns(2)
with col1:
    st.write("### Most Common Batting Styles")
    fig_bat = px.bar(df2_batting, x="COUNT", y="BATTING_STYLE", orientation='h', title="Batting Styles")
    st.plotly_chart(fig_bat)
with col2:
    st.write("### Most Common Bowling Styles")
    fig_bowl = px.bar(fd2_bowling, x="COUNT", y="BOWLING_STYLE", orientation='h', title="Bowling Styles")
    st.plotly_chart(fig_bowl)

st.header("Player by Country")
df4 = session.sql("SELECT COUNTRY, COUNT(*) AS PLAYER_COUNT FROM CDW_LAKEHOUSE.GOLD.MATCHES_SQUADS_INFO GROUP BY COUNTRY ORDER BY PLAYER_COUNT DESC").to_pandas()
fig = px.bar(df4, x="PLAYER_COUNT", y="COUNTRY", orientation='h', title="Players by Country")
st.plotly_chart(fig)

st.header("Team Composition Breakdown")
df5= session.sql("SELECT ROLE, COUNT(*) AS PLAYER_COUNT FROM CDW_LAKEHOUSE.GOLD.MATCHES_SQUADS_INFO GROUP BY ROLE").to_pandas()
fig = px.pie(df5, names='ROLE', values='PLAYER_COUNT', title="Team Composition Breakdown")
st.plotly_chart(fig)




daily_matches_query = "SELECT * FROM DAILY_MATCHES_STATISTICS"
ball_by_ball_query = "SELECT * FROM MATCHES_BALL_BY_BALL_INSIGHTS"
squads_query = "SELECT * FROM MATCHES_SQUADS_INFO"
daily_matches = session.sql(daily_matches_query).to_pandas()
ball_by_ball = session.sql(ball_by_ball_query).to_pandas()
squads = session.sql(squads_query).to_pandas()
st.title("Cricket Match Analytics Dashboard")

st.subheader("Team Squad Strength")
squad_strength = squads.groupby('TEAM_NAME').size().reset_index(name='PLAYERS_COUNT')
fig1 = px.bar(squad_strength, x='TEAM_NAME', y='PLAYERS_COUNT', title='Players per Team')
st.plotly_chart(fig1)

st.subheader("Player Experience")
player_experience = squads.groupby(['PLAYER_NAME', 'TEAM_NAME']).size().reset_index(name='MATCHES_PLAYED')
fig2 = px.scatter(player_experience, x='PLAYER_NAME', y='MATCHES_PLAYED', size='MATCHES_PLAYED', color='TEAM_NAME', title='Player Experience')
st.plotly_chart(fig2)

st.subheader("Runs Progression Over Overs")
runs_per_over = ball_by_ball.groupby('OVER_NUMBER')['RUNS'].sum().reset_index()
fig3 = px.line(runs_per_over, x='OVER_NUMBER', y='RUNS', title='Runs Scored Over Overs')
st.plotly_chart(fig3)

st.subheader("Wickets Lost per Team")
wickets_lost = daily_matches.groupby('TEAM_A')['WICKETS'].sum().reset_index()
fig4 = px.pie(wickets_lost, names='TEAM_A', values='WICKETS', title='Wickets Lost per Team')
st.plotly_chart(fig4)

st.subheader("Win Probability Gauge")
win_probability = daily_matches[['MATCH_WINNER', 'RUNS']].groupby('MATCH_WINNER').sum().reset_index()
fig5 = px.bar(win_probability, x='MATCH_WINNER', y='RUNS', title='Win Probability')
st.plotly_chart(fig5)
st.write("Dashboard built using Snowflake, Streamlit, and Plotly!")
