import dash
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import psycopg2
import psycopg2.sql as sql
import datetime
import dash_bootstrap_templates as dbt

# --- Database Connection Parameters ---
db_connection_params = {
    "dbname": "beatbnk_db",
    "user": "user",
    "password": "X1SOrzeSrk",
    "host": "beatbnk-db-green-0j3yjq.cdgq4essi2q1.ap-southeast-2.rds.amazonaws.com",
    "port": "5432"
}

tables_to_query = [
    "SequelizeMeta", "attendees", "categories", "category_mappings",
    "event_tickets", "events", "follows", "genres", "group_permissions",
    "groups", "interests", "media_files", "media_types",
    "mpesa_stk_push_payments", "otps", "performer_genres",
    "performer_tip_payments", "performer_tips", "performers",
    "permissions", "refresh_tokens", "song_request_payments",
    "song_requests", "tickets", "user_fcm_tokens", "user_groups",
    "user_interests", "user_venue_bookings", "users", "venue_bookings",
    "venues"
]

# --- Global variable to store data (will be updated by interval) ---
global_data = {}

def fetch_data_from_db(db_params, table_names):
    """
    Connects to DB, fetches all records and their column names for specified tables.
    Returns a dictionary of pandas DataFrames.
    """
    connection = None
    data_frames = {}
    try:
        connection = psycopg2.connect(**db_params)
        with connection.cursor() as cursor:
            for table_name in table_names:
                try:
                    query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
                    cursor.execute(query)
                    columns = [desc[0] for desc in cursor.description]
                    records = cursor.fetchall()
                    data_frames[table_name] = pd.DataFrame(records, columns=columns)
                except (Exception, psycopg2.Error) as error:
                    print(f"Error fetching from table '{table_name}': {error}")
                    data_frames[table_name] = pd.DataFrame() # Empty DataFrame on error
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"A critical database error occurred: {error}")
    finally:
        if connection is not None:
            connection.close()
    return data_frames

# Initialize data on application startup
global_data = fetch_data_from_db(db_connection_params, tables_to_query)
print("Initial data fetched successfully.")

# --- Dash App Setup ---
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.VAPOR, dbc.icons.FONT_AWESOME])
server = app.server



# Apply a Bootstrap template
dbt.load_figure_template("flatly")

# --- Layout Components ---

# The main dashboard layout
dashboard_layout = html.Div([
    html.H1("BeatBnk Data Insights Dashboard", className="mb-4 text-center text-primary"),
    html.Hr(className="my-4"),

    # KPIs Row
    dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H5("Total Events", className="card-title text-muted"),
            html.P(id="kpi-total-events", className="card-text fs-2 text-primary fw-bold")
        ]), className="text-center m-2 shadow-sm"), md=3),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H5("Total Users", className="card-title text-muted"),
            html.P(id="kpi-total-users", className="card-text fs-2 text-success fw-bold")
        ]), className="text-center m-2 shadow-sm"), md=3),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H5("Total Performers", className="card-title text-muted"),
            html.P(id="kpi-total-performers", className="card-text fs-2 text-info fw-bold")
        ]), className="text-center m-2 shadow-sm"), md=3),
         dbc.Col(dbc.Card(dbc.CardBody([
            html.H5("Total Tips (KSH)", className="card-title text-muted"),
            html.P(id="kpi-total-tips", className="card-text fs-2 text-warning fw-bold")
        ]), className="text-center m-2 shadow-sm"), md=3),
    ], className="mb-5"), # Add margin bottom for separation

    # Visualizations Rows (5 rows, 2 charts per row)
    dbc.Row([
        dbc.Col(dbc.Card(dcc.Graph(id='event-status-pie'), body=True, className="m-2 shadow-sm"), md=6),
        dbc.Col(dbc.Card(dcc.Graph(id='new-users-line'), body=True, className="m-2 shadow-sm"), md=6),
    ]),
    dbc.Row([
        dbc.Col(dbc.Card(dcc.Graph(id='event-ticket-sales'), body=True, className="m-2 shadow-sm"), md=6),
        dbc.Col(dbc.Card(dcc.Graph(id='event-price-distribution'), body=True, className="m-2 shadow-sm"), md=6),
    ]),
    dbc.Row([
        dbc.Col(dbc.Card(dcc.Graph(id='total-tips-over-time'), body=True, className="m-2 shadow-sm"), md=6),
        dbc.Col(dbc.Card(dcc.Graph(id='top-tipped-performers'), body=True, className="m-2 shadow-sm"), md=6),
    ]),
    dbc.Row([
        dbc.Col(dbc.Card(dcc.Graph(id='total-transactions-over-time'), body=True, className="m-2 shadow-sm"), md=6),
        dbc.Col(dbc.Card(dcc.Graph(id='events-by-category'), body=True, className="m-2 shadow-sm"), md=6),
    ]),
    dbc.Row([
        dbc.Col(dbc.Card(dcc.Graph(id='users-by-registration-month'), body=True, className="m-2 shadow-sm"), md=6),
        dbc.Col(dbc.Card(dcc.Graph(id='venue-booking-status-pie'), body=True, className="m-2 shadow-sm"), md=6),
    ]),
])

# Main application layout
app.layout = html.Div([
    dcc.Location(id='url', refresh=False), # Keep dcc.Location for internal Dash workings, but no routing logic
    dcc.Interval(
        id='interval-component',
        interval=60*1000, # in milliseconds (60 seconds)
        n_intervals=0
    ),
    dbc.Toast(
        "Data refreshed successfully!",
        id="data-refresh-toast",
        header="Data Refresh",
        is_open=False,
        dismissable=True,
        icon="success",
        duration=3000,
        style={"position": "fixed", "top": 66, "right": 10, "width": 350, "zIndex": 9999}, # Ensure toast is on top
    ),
    dashboard_layout # Directly render the dashboard layout
])

# --- Callbacks for Data Refresh ---
@app.callback(
    Output('data-refresh-toast', 'is_open'),
    Output('interval-component', 'n_intervals'), # Reset interval count to avoid overflow
    Input('interval-component', 'n_intervals')
)
def refresh_data(n):
    global global_data
    if n > 0: # Avoid refreshing on initial load (it's already done)
        print(f"Refreshing data at {datetime.datetime.now()}...")
        global_data = fetch_data_from_db(db_connection_params, tables_to_query)
        print("Data refreshed.")
        return True, 0 # Open toast, reset interval count
    return False, 0 # Don't open toast on initial load, reset interval count


# --- Callback for Dashboard KPIs and All 10 Graphs ---
@app.callback(
    [Output('kpi-total-events', 'children'),
     Output('kpi-total-users', 'children'),
     Output('kpi-total-performers', 'children'),
     Output('kpi-total-tips', 'children'),
     Output('event-status-pie', 'figure'),
     Output('new-users-line', 'figure'),
     Output('event-ticket-sales', 'figure'),
     Output('event-price-distribution', 'figure'),
     Output('total-tips-over-time', 'figure'),
     Output('top-tipped-performers', 'figure'),
     Output('total-transactions-over-time', 'figure'),
     Output('events-by-category', 'figure'),
     Output('users-by-registration-month', 'figure'),
     Output('venue-booking-status-pie', 'figure')],
    [Input('interval-component', 'n_intervals')] # Trigger on interval
)
def update_dashboard_visualizations(n):
    # Access global_data (already refreshed by the interval callback)
    events_df = global_data.get('events', pd.DataFrame()).copy()
    users_df = global_data.get('users', pd.DataFrame()).copy()
    performers_df = global_data.get('performers', pd.DataFrame()).copy()
    performer_tips_df = global_data.get('performer_tips', pd.DataFrame()).copy()
    event_tickets_df = global_data.get('event_tickets', pd.DataFrame()).copy()
    categories_df = global_data.get('categories', pd.DataFrame()).copy()
    category_mappings_df = global_data.get('category_mappings', pd.DataFrame()).copy()
    mpesa_payments_df = global_data.get('mpesa_stk_push_payments', pd.DataFrame()).copy()
    venue_bookings_df = global_data.get('venue_bookings', pd.DataFrame()).copy()


    # --- KPIs ---
    total_events = len(events_df)
    total_users = len(users_df)
    total_performers = len(performers_df)
    total_tips = performer_tips_df['tipAmount'].sum() if not performer_tips_df.empty and 'tipAmount' in performer_tips_df.columns else 0

    # --- 10 Visualizations ---

    # 1. Event Status Distribution (Pie Chart)
    event_status_fig = go.Figure()
    if not events_df.empty and 'eventStatus' in events_df.columns:
        status_counts = events_df['eventStatus'].value_counts().reset_index()
        status_counts.columns = ['Status', 'Count']
        event_status_fig = px.pie(status_counts, values='Count', names='Status',
                                   title='Distribution of Event Status', hole=0.3)
    else:
        event_status_fig.add_annotation(text="No event status data available.",
                                        xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        event_status_fig.update_layout(title='Distribution of Event Status', xaxis_visible=False, yaxis_visible=False)


    # 2. New Users Registered Over Time (Line Chart)
    new_users_fig = go.Figure()
    if not users_df.empty and 'createdAt' in users_df.columns:
        users_df['registration_date'] = pd.to_datetime(users_df['createdAt']).dt.to_period('M')
        monthly_users = users_df.groupby('registration_date').size().reset_index(name='count')
        monthly_users['registration_date'] = monthly_users['registration_date'].astype(str)
        new_users_fig = px.line(monthly_users, x='registration_date', y='count',
                                title='New Users Registered Over Time (Monthly)',
                                labels={'registration_date': 'Month', 'count': 'New Users'})
        new_users_fig.update_xaxes(type='category')
    else:
        new_users_fig.add_annotation(text="No user registration data available.",
                                        xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        new_users_fig.update_layout(title='New Users Registered Over Time (Monthly)', xaxis_visible=False, yaxis_visible=False)

    # 3. Top 10 Events by Tickets Sold (Bar Chart)
    event_ticket_sales_fig = go.Figure()
    # MODIFIED: Added 'eventName' check to events_df
    if (not events_df.empty and 'eventName' in events_df.columns and
        not event_tickets_df.empty and 'totalTickets' in event_tickets_df.columns and 'availableTickets' in event_tickets_df.columns):
        events_with_tickets_df = pd.merge(events_df, event_tickets_df, left_on='id', right_on='eventId', how='left', suffixes=('_event', '_ticket'))
        # Ensure eventName_event column exists after merge, especially if merge resulted in empty dataframe
        if 'eventName_event' in events_with_tickets_df.columns and not events_with_tickets_df.empty:
            events_with_tickets_df['tickets_sold'] = events_with_tickets_df['totalTickets'] - events_with_tickets_df['availableTickets']
            sales_by_event = events_with_tickets_df.groupby('eventName_event')['tickets_sold'].sum().reset_index()
            sales_by_event = sales_by_event.sort_values(by='tickets_sold', ascending=False).head(10)
            event_ticket_sales_fig = px.bar(sales_by_event, x='eventName_event', y='tickets_sold',
                                            title='Top 10 Events by Tickets Sold',
                                            labels={'eventName_event': 'Event Name', 'tickets_sold': 'Tickets Sold'})
        else:
            event_ticket_sales_fig.add_annotation(text="Merged event data missing 'eventName_event' or is empty.",
                                                xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
            event_ticket_sales_fig.update_layout(title='Top 10 Events by Tickets Sold', xaxis_visible=False, yaxis_visible=False)
    else:
        event_ticket_sales_fig.add_annotation(text="Required data for ticket sales not available.",
                                            xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        event_ticket_sales_fig.update_layout(title='Top 10 Events by Tickets Sold', xaxis_visible=False, yaxis_visible=False)


    # 4. Distribution of Event Ticket Prices (Histogram)
    price_dist_fig = go.Figure()
    if not event_tickets_df.empty and 'price' in event_tickets_df.columns:
        price_dist_fig = px.histogram(event_tickets_df, x='price', nbins=20,
                                      title='Distribution of Event Ticket Prices',
                                      labels={'price': 'Ticket Price (KSH)', 'count': 'Number of Tickets'})
    else:
        price_dist_fig.add_annotation(text="No ticket price data available.",
                                      xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        price_dist_fig.update_layout(title='Distribution of Event Ticket Prices', xaxis_visible=False, yaxis_visible=False)


    # 5. Total Tips Amount Over Time (Line Chart)
    total_tips_over_time_fig = go.Figure()
    if not performer_tips_df.empty and 'createdAt' in performer_tips_df.columns and 'tipAmount' in performer_tips_df.columns:
        performer_tips_df['tip_date'] = pd.to_datetime(performer_tips_df['createdAt']).dt.date
        daily_tips = performer_tips_df.groupby('tip_date')['tipAmount'].sum().reset_index()
        total_tips_over_time_fig = px.line(daily_tips, x='tip_date', y='tipAmount',
                                           title='Total Tips Amount Over Time',
                                           labels={'tip_date': 'Date', 'tipAmount': 'Total Tip Amount (KSH)'})
    else:
        total_tips_over_time_fig.add_annotation(text="No tips data available.",
                                                xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        total_tips_over_time_fig.update_layout(title='Total Tips Amount Over Time', xaxis_visible=False, yaxis_visible=False)

    # 6. Top 10 Tipped Performers (Bar Chart)
    top_tipped_performers_fig = go.Figure()
    if not performer_tips_df.empty and not performers_df.empty and 'performerId' in performer_tips_df.columns and 'tipAmount' in performer_tips_df.columns:
        # Merge to get performer's user ID, then user's email for name
        performer_tips_merged = pd.merge(performer_tips_df, performers_df[['id', 'userId']], left_on='performerId', right_on='id', how='left')
        if not users_df.empty and 'userId' in performer_tips_merged.columns:
            # Check if 'email' column exists in users_df before attempting to use it
            if 'email' in users_df.columns:
                user_emails = users_df.set_index('id')['email'].to_dict()
                performer_tips_merged['performer_email'] = performer_tips_merged['userId'].map(user_emails).fillna('Unknown User')
                # Ensure 'performer_email' column exists and is not all 'Unknown User' before grouping
                if 'performer_email' in performer_tips_merged.columns and not performer_tips_merged['performer_email'].eq('Unknown User').all():
                    tips_by_performer = performer_tips_merged.groupby('performer_email')['tipAmount'].sum().reset_index()
                    tips_by_performer = tips_by_performer.sort_values(by='tipAmount', ascending=False).head(10)
                    top_tipped_performers_fig = px.bar(tips_by_performer, x='performer_email', y='tipAmount',
                                                       title='Top 10 Tipped Performers (by Email)',
                                                       labels={'performer_email': 'Performer Email', 'tipAmount': 'Total Tip Amount (KSH)'})
                else:
                    top_tipped_performers_fig.add_annotation(text="Performer email data not suitable for grouping.",
                                                            xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
                    top_tipped_performers_fig.update_layout(title='Top 10 Tipped Performers (by Email)', xaxis_visible=False, yaxis_visible=False)
            else:
                top_tipped_performers_fig.add_annotation(text="Users table missing 'email' column.",
                                                        xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
                top_tipped_performers_fig.update_layout(title='Top 10 Tipped Performers (by Email)', xaxis_visible=False, yaxis_visible=False)
        else:
            top_tipped_performers_fig.add_annotation(text="No user ID data for performer names or performer tips data missing.",
                                                    xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
            top_tipped_performers_fig.update_layout(title='Top 10 Tipped Performers (by Email)', xaxis_visible=False, yaxis_visible=False)
    else:
        top_tipped_performers_fig.add_annotation(text="No performer tips or performer data available.",
                                                xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        top_tipped_performers_fig.update_layout(title='Top 10 Tipped Performers (by Email)', xaxis_visible=False, yaxis_visible=False)


    # 7. Total Transaction Amount Over Time (Line Chart)
    total_transactions_over_time_fig = go.Figure()
    if not mpesa_payments_df.empty and 'createdAt' in mpesa_payments_df.columns and 'transactionAmount' in mpesa_payments_df.columns:
        mpesa_payments_df['transaction_date'] = pd.to_datetime(mpesa_payments_df['createdAt']).dt.date
        daily_transactions = mpesa_payments_df.groupby('transaction_date')['transactionAmount'].sum().reset_index()
        total_transactions_over_time_fig = px.line(daily_transactions, x='transaction_date', y='transactionAmount',
                                                    title='Total Transaction Amount Over Time (Mpesa STK Push)',
                                                    labels={'transaction_date': 'Date', 'transactionAmount': 'Total Amount (KSH)'})
    else:
        total_transactions_over_time_fig.add_annotation(text="No Mpesa STK Push payments data available.",
                                                        xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        total_transactions_over_time_fig.update_layout(title='Total Transaction Amount Over Time (Mpesa STK Push)', xaxis_visible=False, yaxis_visible=False)


    # 8. Events by Category (Bar Chart)
    events_by_category_fig = go.Figure()
    # MODIFIED: Added checks for 'name' in categories_df columns
    if (not events_df.empty and not category_mappings_df.empty and
        not categories_df.empty and 'name' in categories_df.columns):
        events_categories = pd.merge(events_df, category_mappings_df, left_on='id', right_on='eventId', how='inner')
        events_categories = pd.merge(events_categories, categories_df[['id', 'name']], left_on='categoryId', right_on='id', how='inner', suffixes=('_event', '_category'))
        if 'name_category' in events_categories.columns: # Check if merge resulted in this column
            events_categories.rename(columns={'name_category': 'categoryName'}, inplace=True)
            if not events_categories.empty and 'categoryName' in events_categories.columns: # Ensure categoryName is present and not empty
                category_counts = events_categories['categoryName'].value_counts().reset_index()
                category_counts.columns = ['Category', 'Count']
                events_by_category_fig = px.bar(category_counts, x='Category', y='Count',
                                                title='Number of Events by Category',
                                                labels={'Count': 'Number of Events'})
            else:
                events_by_category_fig.add_annotation(text="Category name data missing or empty after merge.",
                                                      xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
                events_by_category_fig.update_layout(title='Number of Events by Category', xaxis_visible=False, yaxis_visible=False)
        else:
            events_by_category_fig.add_annotation(text="Category name column not found after merge.",
                                                  xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
            events_by_category_fig.update_layout(title='Number of Events by Category', xaxis_visible=False, yaxis_visible=False)
    else:
        events_by_category_fig.add_annotation(text="Required data for events by category not available.",
                                              xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        events_by_category_fig.update_layout(title='Number of Events by Category', xaxis_visible=False, yaxis_visible=False)


    # 9. Users by Registration Month/Year (Bar Chart)
    users_by_registration_month_fig = go.Figure()
    if not users_df.empty and 'createdAt' in users_df.columns:
        users_df['registration_month'] = pd.to_datetime(users_df['createdAt']).dt.to_period('M')
        monthly_users_count = users_df.groupby('registration_month').size().reset_index(name='count')
        monthly_users_count['registration_month'] = monthly_users_count['registration_month'].astype(str)
        users_by_registration_month_fig = px.bar(monthly_users_count, x='registration_month', y='count',
                                                  title='Users Registered by Month',
                                                  labels={'registration_month': 'Registration Month', 'count': 'Number of Users'})
        users_by_registration_month_fig.update_xaxes(type='category')
    else:
        users_by_registration_month_fig.add_annotation(text="No user registration data available.",
                                                        xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        users_by_registration_month_fig.update_layout(title='Users Registered by Month', xaxis_visible=False, yaxis_visible=False)


    # 10. Venue Booking Status Distribution (Pie Chart)
    venue_booking_status_fig = go.Figure()
    if not venue_bookings_df.empty and 'bookingStatus' in venue_bookings_df.columns:
        status_counts = venue_bookings_df['bookingStatus'].value_counts().reset_index()
        status_counts.columns = ['Status', 'Count']
        venue_booking_status_fig = px.pie(status_counts, values='Count', names='Status',
                                           title='Distribution of Venue Booking Status', hole=0.3)
    else:
        venue_booking_status_fig.add_annotation(text="No venue booking status data available.",
                                                xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        venue_booking_status_fig.update_layout(title='Distribution of Venue Booking Status', xaxis_visible=False, yaxis_visible=False)


    return (
        f"{total_events}",
        f"{total_users}",
        f"{total_performers}",
        f"Ksh {total_tips:,.2f}",
        event_status_fig,
        new_users_fig,
        event_ticket_sales_fig,
        price_dist_fig,
        total_tips_over_time_fig,
        top_tipped_performers_fig,
        total_transactions_over_time_fig,
        events_by_category_fig,
        users_by_registration_month_fig,
        venue_booking_status_fig
    )


# --- Main execution ---
if __name__ == '__main__':
    app.run(debug=True, port=8050)
