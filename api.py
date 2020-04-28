from frappeclient import FrappeClient
import pyodbc 
import sys, json
import datetime
from datetime import datetime
from socket import error as SocketError, timeout as SocketTimeout

"""
    TODO
    
      - Batch Search: Get list to test against: items, etc
        - get_all list
        - 

      - stock deduction
        - costing / valuation

    Resilience
    - Connection testing
        - User alert after a while

    v2
    - updated sql transactions
      - Batch search: Get list to test against: items, etc
      - Trigger:
      - Use dates
      - update atrributes: Price, UOM, Group -> Update
"""

# rest connection
def http_connection():

    connection = []
    try:
        connection = FrappeClient("https://meraki.erp.co.zm", "api", "Meraki55%%")

    except SocketTimeout as st:
        print("Connection to %s timed out. (timeout=%s)" % (st.host, st.timeout))
        connection = False

    except SocketError as e:
        # print("Failed to establish a new connection: %s" % e)
        print("Failed to establish a network connection")
        connection = False

    return connection

def sql(sql):    
    # db connection
    db = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                      "Server=localhost;"
                      "Database=Meraki Centro;"
                      "Trusted_Connection=yes;")
    
    cursor = db.cursor()
    cursor.fast_executemany = True

    result = cursor.execute(sql)

    if (sys._getframe().f_back.f_code.co_name == "finish"):
        cursor.commit()
        cursor.close()
        print("Sync complete")

    return result

def sql_write(sql):    
    # db connection
    server = 'tcp:localhost' 
    database = 'Meraki Centro' 
    username = 'sa' 
    password = '4444meraki666666' 
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()

    result = cursor.execute(sql)
    cursor.commit()

    return result



def test_network(https):
    if https:
        users = https.get_list('User', fields = ['name', 'first_name', 'last_name'], filters = {'user_type':'System User'})        

        for u in users:
            print(u['first_name'] + "  :  " + str(u))
  
    else:
        print("No connection")

def is_not_empty(my_string):
    return bool(my_string and my_string.strip())
    
def get_groups():
    return sql('SELECT * FROM dbo.GroupCodesView')

def set_groups():
    
    https = http_connection()

    if (https):

        for g in get_groups():
            new = str(g[1])
            if is_not_empty(new):
                grp = https.get_value("Item Group", "parent", {"item_group_name": new})

                if not grp:
                    ins = https.insert({"doctype": "Item Group",
                                    "parent": "Foods",
                                    "item_group_name": new
                                })
                    print(new + ": inserted")
                else:
                    print(new + ": skipped")

def get_uoms():
    return sql('SELECT * FROM dbo.UOMView')

def set_uoms():
    
    https = http_connection()
    if (https):

        for u in get_uoms():        
            new = str(u[1])

            if is_not_empty(new):
                grp = https.get_value("UOM", "uom_name", {"uom_name": new})

                if not grp:
                    ins = https.insert({"doctype": "UOM",
                                    "uom_name": new
                                })
                    print(new + ": inserted")
                else:
                    print(new + ": skipped")


def get_item(item):
    return sql("SELECT Name AS item, GroupCode AS itemgroup, UOM AS uom FROM dbo.PricedUOMItem WHERE Name ='" + item + "'")

def set_items():
    """
    Pulls items from SambaPOS and Syncs to ERPNext

    - Only upload items not in ERPNext
        - Get_all ERPNext list
        - Compare with SQL result
    """

    https = http_connection()

    if (https):

        db_items = sql('SELECT Name AS item, GroupCode AS itemgroup, UOM AS uom, Modified, Id FROM dbo.PricedUOMItem') # WHERE ItemUploaded = 0

        filters={'title': ('like', 's')}
        
        erp_items = https.get_list('Items',
                        filters={'title': ('like', 's')},
                        fields=["title", "public"]                        
                    )

    """    for i in db_items:
            item = str(i[0])
            group = str(i[1])
            uom = str(i[2])
            item_id = str(i[4])

            chars = ["<", ">", ",", "'", '"', "/", "%", "^", "*", "`"] 

            for s in chars: # remove all the 8s 
                item = item.replace(str(s), '')
           
            item = item.replace("&amp;", "&")
        
            if not uom or not is_not_empty(uom) or uom == "None": 
                uom = "Normal"
            elif (uom == "Normal" or uom == "normal"):
                pass
            else:
                # append uom to name
                item = item + " " + uom
                uom = "Normal"
                
            # Insert group if does not exist 
            if not group or not is_not_empty(group) or group == "None": 
                group = "Others"
                    
            if is_not_empty(item):
                itm = https.get_value("Item", "item_name", {"item_name": item})
                
                if not itm:
                    ins = https.insert({ "doctype": "Item",
                        "item_code": item,
                        "item_group": group,
                        "stock_uom": uom
                    })

                    print(item + ": inserted")
                    sql_write("UPDATE dbo.MenuItems SET ItemUploaded = 1 WHERE Id = '" + item_id + "';")
                else:
                    print(item + ": skipped")
    """        

def get_paid_tickets():
    return sql('SELECT * FROM dbo.PaidTicketView WHERE TicketUploaded = 0')

def insert_dependancies():
    pass

def update_dependancies():
    pass

def get_settings():
    return sql('SELECT * FROM dbo.erpnext_settings')

def create_invoices():
    # get settings: restaurant name
    # get invoices
    settings = ""


def get_invoice_items(ticket_id, income_account, cost_centre):
   
    items = []

    amount = 0
    rate = 0
    item_name = ""

    item_query = sql("SELECT TicketId, MenuItemName, PortionName, Price, Quantity, OrderNumber, oTotal FROM [Meraki Centro].[dbo].[OrdersView] WHERE TicketId ='"+ ticket_id +"'")

    for i in item_query:

        amount = float(i[6])
        rate = float(i[3])
        item_name = str(i[1])

        for f in get_item(item_name):
            item_name = str(f[0])
            group = str(f[1])
            uom = str(f[2])

            chars = ["<", ">", ",", "'", '"', "/", "%", "^", "*", "`"]

            # remove all the special characters
            for s in chars: 
                item_name = item_name.replace(str(s), '') 

            item_name = item_name.replace("&amp;", "&")

            if not uom or not is_not_empty(uom) or uom == "None": 
                uom = "Normal"
            elif (uom == "Normal" or uom == "normal"):
                pass
            else:
                # append uom to name
                item_name = item_name + " " + uom
                uom = "Normal"
                
            # Insert group if does not exist 
            if not group or not is_not_empty(group) or group == "None": 
                group = "Others"

            items.append({            
                "item_name": item_name,
                "item_code": item_name,
                "description": item_name,
                "uom": uom,
                "qty": int(i[4]),
                "stock_qty":int(i[4]),
                "conversion_factor":1,
                "rate": rate,
                "base_rate": rate,
                "amount": amount,
                "base_amount": amount,
                "income_account": income_account,
                "cost_center": cost_centre
            })
        
    return items

def create_fiscal_year():
    https = http_connection()
    #ow = datetime.datetime.now()  
    #print (date.year, date.month, date.day, date.hour, date.minute, date.second)
    if (https):
    
        year = "2022" #str(date.year)
        year_start_date = str(datetime.strptime('01-01-' + year, '%d-%m-%Y').date())
        year_end_date = str(datetime.strptime('31-12-' + year, '%d-%m-%Y').date())

        fiscal_year = https.get_list( 'Fiscal Year', fields = ['year', 'year_start_date', 'year_end_date'], filters = {'year': ('Like', '%'+ year + '%') } )

        if not fiscal_year:
            response = https.insert({
                "doctype": "Fiscal Year",
                "year": year,
                "year_start_date": year_start_date,
                "year_end_date": year_end_date
            })

            # fiscal_year = datetime.strptime('01-01-'+ year, '%d-%m-%Y').strftime('%d-%m-%Y')
            fiscal_year = https.get_doc("Fiscal Year", year)      

def insert_invoices():
    """              
        get invoices  from db and uses get_items() to get orders for each invoice
        insert_invoices
        update inserted status for ticket
    """
    
    https = http_connection()

    if (https):    

        # get settings
        default_company = ""
        default_customer = ""
        last_update = ""
        territory = ""
        debit_to = ""
        tax_rate = 0
        restaurant = ""
        income_account = ""
        cost_centre = ""

        settings = get_settings()

        for row in settings:
            if (row[0] == "default_company"):
                default_company = str(row[1])
            elif (row[0] == "default_customer"):
                default_customer = str(row[1])
            elif (row[0] == "last_update"):
                last_update = str(row[1])
            elif (row[0] == "territory"):
                territory = str(row[1])
            elif (row[0] == "debit_to"):
                debit_to = str(row[1])
            elif (row[0] == "tax_rate"):
                tax_rate = int(row[1])
            elif (row[0] == "restaurant_name"):
                restaurant = str(row[1])
            elif (row[0] == "income_account"):
                income_account = str(row[1])
            elif (row[0] == "cost_centre"):
                cost_centre = str(row[1])

        tickets = get_paid_tickets()

        for t in tickets:

            ticket_id = str(t[0])
            items = get_invoice_items(ticket_id, income_account, cost_centre)
            
            customer_name = ""
            if (t[-2] == "Walk-in"):
                customer_name = default_customer
            else:
                customer_name = t[-2]

            # check customer existence and create customer if required        
            customer = https.get_list( 'Customer', fields = ['customer_name'], filters = {'customer_name': 'customer_name'} )

            if not customer:
                response = https.insert({
                    "doctype":"Customer",
                    "customer_name": customer_name,
                    "territory": territory,
                    "customer_group": "Individual",
                    "company": default_company,
                    "mobile_no": "000000"
                })
            
            date = datetime.strptime(str(t[4]), '%b %d %Y %I:%M%p')
            posting_date = date.strftime('%Y-%m-%d')

            grand_total = float(t[7])
            tax = grand_total * (tax_rate/100)
            net_total = grand_total - tax

            restaurant_table = str(t[-1])
            sambapos_ticket = str(t[0])

            resonse = https.insert({
                        
                "doctype": "Sales Invoice",
                "naming_series": "ACC-SINV-.YYYY.-",
                "company": default_company,
                "sambapos_ticket": sambapos_ticket,
                "set_posting_time": "1",
                "posting_date": posting_date,
                "is_pos": "1", 
                "conversion_rate": 1.0, 
                "currency": "ZMW", 
                "debit_to": debit_to,
                "customer": customer_name,
                "customer_name": customer_name,
                "grand_total": grand_total, 
                "base_grand_total": grand_total, 
                "net_total": net_total, 
                "base_net_total": net_total, 
                "base_rounded_total": grand_total,
                "rounded_total": grand_total, 
                "plc_conversion_rate": 1.0, 
                "price_list_currency": "ZMW", 
                "price_list_name": "Standard Selling", 
                "restaurant_name": restaurant,
                "restaurant_table": restaurant_table,
                "docstatus":1,
                "items": items,
                "payments":[{
                    "mode_of_payment":"Cash",
                    "amount":grand_total,
                    "base_amount":grand_total,
                    "account": income_account
                }],
                "taxes": [{
                    "account_head": "VAT - MCB", 
                    "charge_type": "On Net Total", 
                    "description": "VAT @ 16.0",
                    "tax_amount": float(tax),
                    "rate": tax_rate,
                    "included_in_print_rate": 1
                }],
                
            })
            if resonse['name']:
                print(resonse['name'], posting_date+" total: "+str(grand_total)+" restaurant_name: "+restaurant)
                
                sql_write("UPDATE dbo.Tickets SET TicketUploaded = 1 WHERE Id = '" + sambapos_ticket + "';")

# close db connection, update last update time
def finish():
    # set last update time
    sql("UPDATE dbo.erpnext_settings SET value = GETDATE() WHERE name = 'last_update';")

    
def start():

    """
    - Get masters: UOM, Group, Item
    - Insert new invoices

    TODO
    - Batch Search: Get list to test against: items, etc
        - get_all list
        -  
    """

    print("Sync starting")
    # get_settings()
    # set_uoms()
    # set_groups()
    set_items()

    

    insert_invoices()
    finish()


start()