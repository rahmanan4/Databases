import random
import time
import server
from datetime import date


def createWarehouseTable(numWarehouses):
    warehouseTable = []
    streetList = ['245 Blue Street', '324 Red Street', '435 Green Street']
    cityList = ['San Jose', 'Sunnyvale', 'Mountain View']
    zipList = [95003, 52034, 49563]
    posNegList = [1, -1]
    for i in range(1, numWarehouses+1):
        randW_STREET_1 = random.randint(0, (len(streetList) - 1))
        randW_STREET_2 = random.randint(0, (len(streetList) - 1))
        randW_CITY = random.randint(0, (len(cityList) - 1))
        randW_ZIP = random.randint(0, (len(zipList) - 1))
        warehouseTable.append({'W_ID': i,
                               'W_NAME': ('Warehouse ' + str(i)),
                               'W_STREET_1': streetList[randW_STREET_1],
                               'W_STREET_2': streetList[randW_STREET_2],
                               'W_CITY': cityList[randW_CITY],
                               'W_STATE': 'CA',
                               'W_ZIP': zipList[randW_ZIP],
                               'W_TAX': (posNegList[random.randint(0, (len(posNegList) - 1))] * round(random.random(), 4)),
                               'W_YTD': (posNegList[random.randint(0, (len(posNegList) - 1))] * round(random.uniform(1000000000, 9999999999), 2))})
    return warehouseTable


def createDistrictTable(numOfWarehouses):
    districtTable = []
    numDistricts = 10
    streetList = ['245 Blue Street', '324 Red Street', '435 Green Street']
    cityList = ['San Jose', 'Sunnyvale', 'Mountain View']
    zipList = [95003, 52034, 49563]
    posNegList = [1, -1]
    districtWarehouseNum = 1
    for _ in range(1, numOfWarehouses+1):
        for i in range(1, numDistricts+1):
            randD_STREET_1 = random.randint(0, (len(streetList)-1))
            randD_STREET_2 = random.randint(0, (len(streetList)-1))
            randD_CITY = random.randint(0, (len(cityList)-1))
            randD_ZIP = random.randint(0, (len(zipList)-1))
            districtTable.append({'D_ID': i,
                                  'D_W_ID': districtWarehouseNum,
                                  'D_NAME': ('District ' + str(i)),
                                  'D_STREET_1': streetList[randD_STREET_1],
                                  'D_STREET_2': streetList[randD_STREET_2],
                                  'D_CITY': cityList[randD_CITY],
                                  'D_STATE': 'CA',
                                  'D_ZIP': zipList[randD_ZIP],
                                  'D_TAX': (posNegList[random.randint(0, (len(posNegList) - 1))] * round(random.random(), 4)),
                                  'D_YTD': (posNegList[random.randint(0, (len(posNegList) - 1))] * round(random.uniform(1000000000, 9999999999), 2)),
                                  'D_NEXT_O_ID': i+1})
        districtWarehouseNum += 1
    return districtTable


def createCustomerTable(numOfWarehouses):
    customerTable = []
    numOfDistricts = 10
    numCustomers = 3000
    firstNameList = ['Alice', 'Bob', 'Eve']
    middleNameList = ['Ah', 'Bt', 'Es']
    lastNameList = ['Robinson', 'Nguyen', 'Juarez']
    streetList = ['485 Orange Street', '3056 Black Road', '3239 Brown Court']
    cityList = ['San Jose', 'Sunnyvale', 'Mountain View']
    zipList = [95003, 52034, 49563]
    phoneList = ['665-340-3405', '534-349-6075', '394-596-2345']
    creditList = ['GC', 'BC']
    posNegList = [1, -1]
    customerWarehouseNum = 1
    customerDistrictNum = 1
    for k in range(1, numOfWarehouses+1):
        for j in range(1, numOfDistricts+1):
            for i in range(1, numCustomers+1):
                randC_FIRST = random.randint(0, (len(firstNameList)-1))
                randC_MIDDLE = random.randint(0, (len(middleNameList)-1))
                randC_LAST = random.randint(0, (len(lastNameList)-1))
                randC_STREET_1 = random.randint(0, (len(streetList) - 1))
                randC_STREET_2 = random.randint(0, (len(streetList) - 1))
                randC_CITY = random.randint(0, (len(cityList) - 1))
                randC_ZIP = random.randint(0, (len(zipList) - 1))
                randC_PHONE = random.randint(0, (len(phoneList) - 1))
                randC_CREDIT = random.randint(0, (len(creditList) - 1))
                customerTable.append({'C_ID': i,
                                      'C_D_ID': customerDistrictNum,
                                      'C_W_ID': customerWarehouseNum,
                                      'C_FIRST': firstNameList[randC_FIRST],
                                      'C_MIDDLE': middleNameList[randC_MIDDLE],
                                      'C_LAST': lastNameList[randC_LAST],
                                      'C_STREET_1': streetList[randC_STREET_1],
                                      'C_STREET_2': streetList[randC_STREET_2],
                                      'C_CITY': cityList[randC_CITY],
                                      'C_STATE': 'CA',
                                      'C_ZIP': zipList[randC_ZIP],
                                      'C_PHONE': phoneList[randC_PHONE],
                                      'C_SINCE': date.today().strftime("%d/%m/%Y"),
                                      'C_CREDIT': creditList[randC_CREDIT],
                                      'C_CREDIT_LIM': (posNegList[random.randint(0, (len(posNegList) - 1))] * round(random.uniform(1000000000, 9999999999), 2)),
                                      'C_DISCOUNT': (posNegList[random.randint(0, (len(posNegList) - 1))] * round(random.random(), 4)),
                                      'C_BALANCE': (posNegList[random.randint(0, (len(posNegList) - 1))] * round(random.uniform(1000000000, 9999999999), 2)),
                                      'C_YTD_PAYMENT': (posNegList[random.randint(0, (len(posNegList) - 1))] * round(random.uniform(1000000000, 9999999999), 2)),
                                      'C_PAYMENT_CNT': int(random.uniform(1000, 9999)),
                                      'C_DELIVERY_CNT': int(random.uniform(1000, 9999)),
                                      'C_DATA': 'Miscellaneous Information'})
                if i == 3000:
                    customerDistrictNum += 1
        customerWarehouseNum += 1
    return customerTable


def createHistoryTable():
    historyTable = []
    return historyTable


def addToHistoryTable(historyTable, H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA):
    historyTable.append({'H_C_ID': H_C_ID,
                         'H_C_D_ID': H_C_D_ID,
                         'H_C_W_ID': H_C_W_ID,
                         'H_D_ID': H_D_ID,
                         'H_W_ID': H_W_ID,
                         'H_DATE': H_DATE,
                         'H_AMOUNT': H_AMOUNT,
                         'H_DATA': H_DATA})


def createNewOrderTable():
    newOrderTable = []
    return newOrderTable


def addNewOrder(newOrderTable, NO_O_ID, NO_D_ID, NO_W_ID):
    newOrderTable.append({'NO_O_ID': NO_O_ID,
                          'NO_D_ID': NO_D_ID,
                          'NO_W_ID': NO_W_ID})


def deleteNewOrder(newOrderTable, NO_O_ID, NO_D_ID, NO_W_ID):
    for newOrder in newOrderTable:
        if NO_O_ID == newOrder['NO_O_ID'] and NO_D_ID == newOrder['NO_D_ID'] and NO_W_ID == newOrder['NO_W_ID']:
            newOrderTable.remove(newOrder)


def createOrderTable():
    orderTable = []
    return orderTable


def addOrder(orderTable, O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL):
    orderTable.append({'O_ID': O_ID,
                       'O_D_ID': O_D_ID,
                       'O_W_ID': O_W_ID,
                       'O_C_ID': O_C_ID,
                       'O_ENTRY_D': O_ENTRY_D,
                       'O_CARRIER_ID': O_CARRIER_ID,
                       'O_OL_CNT': O_OL_CNT,
                       'O_ALL_LOCAL': O_ALL_LOCAL})


def createOrderLineTable():
    orderLineTable = []
    return orderLineTable


def addOrderLine(orderLineTable, OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D,
                 OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO):
    orderLineTable.append({'OL_O_ID': OL_O_ID,
                           'OL_D_ID': OL_D_ID,
                           'OL_W_ID': OL_W_ID,
                           'OL_NUMBER': OL_NUMBER,
                           'OL_I_ID': OL_I_ID,
                           'OL_SUPPLY_W_ID': OL_SUPPLY_W_ID,
                           'OL_DELIVERY_D': OL_DELIVERY_D,
                           'OL_QUANTITY': OL_QUANTITY,
                           'OL_AMOUNT': OL_AMOUNT,
                           'OL_DIST_INFO': OL_DIST_INFO})


def createItemTable():
    itemTable = []
    itemNum = 100000
    for i in range(1, itemNum+1):
        itemTable.append({'I_ID': i,
                          'I_IM_ID': i,
                          'I_NAME': ('Item ' + str(i)),
                          'I_PRICE': round(random.uniform(100, 1000), 2),
                          'I_DATA': 'brand info'})
    return itemTable


def createStockTable(numWarehouse):
    stockTable = []
    numStocks = 100000
    stockWarehouseNum = 1
    posNegList = [1, -1]
    for _ in range(1, numWarehouse+1):
        for i in range(1, numStocks+1):
            stockTable.append({'S_I_ID': i,
                               'S_W_ID': stockWarehouseNum,
                               'S_QUANTITY': posNegList[random.randint(0, (len(posNegList) - 1))] * random.randint(1000, 10000),
                               'S_DIST_01': 'iiiiooooppppiiiioooopppp',
                               'S_DIST_02': 'iiiiooooppppiiiioooopppp',
                               'S_DIST_03': 'iiiiooooppppiiiioooopppp',
                               'S_DIST_04': 'iiiiooooppppiiiioooopppp',
                               'S_DIST_05': 'iiiiooooppppiiiioooopppp',
                               'S_DIST_06': 'iiiiooooppppiiiioooopppp',
                               'S_DIST_07': 'iiiiooooppppiiiioooopppp',
                               'S_DIST_08': 'iiiiooooppppiiiioooopppp',
                               'S_DIST_09': 'iiiiooooppppiiiioooopppp',
                               'S_DIST_10': 'iiiiooooppppiiiioooopppp',
                               'S_YTD': random.randint(10000000, 100000000),
                               'S_ORDER_CNT': random.randint(1000, 10000),
                               'S_REMOTE_CNT': random.randint(1000, 10000),
                               'S_DATA': 'Make information'})
        stockWarehouseNum += 1
    return stockTable


# Calculate nonuniform random number for C_LAST, C_ID, or OL_I_ID
def NURand(A, x, y):
    C = random.randint(0, A)
    result = (((random.randint(0, A) or random.randint(x, y)) + C) % (y - x + 1)) + x
    return result


def get_warehouse(warehouseTable):
    for warehouse in warehouseTable:
        W_TAX = warehouse['W_TAX']
        return W_TAX

def newOrderTransaction(W_ID, D_W_ID, D_ID, C_W_ID, C_D_ID, C_ID, O_ENTRY_D, I_IDS, I_W_IDS, I_QTYS,
                        warehouseTable, districtTable, customerTable, itemTable, newOrderTable, orderLineTable,
                        orderTable, stockTable):
    stockTableInitial = stockTable.copy()
    # ------------------------------------------------------------------------------------------------------------------
    # Getting information from wareHouseTable, districtTable, customerTable
    # ------------------------------------------------------------------------------------------------------------------
    for warehouse in warehouseTable:
        if W_ID == warehouse['W_ID']:
            W_TAX = warehouse['W_TAX']
            break
        # if more than one node, and the warehouse isn't the right node, must search for proper W_ID
        else:
            for addr in server.mem_info:
                if server.mem_info[addr] > W_ID and W_ID > server.mem_info[addr] - 1:
                    # found the node where warehouse is
                    for connection in server.connections:
                        # look for the socket to that node
                        if connection.getpeername() == addr:
                            # send the op
                            connection.send(str.encode('getwarehousetax'))
                            # return None when operation is sent to other node.
                            return None
                        if connection.getsockname() == addr:
                            # send the op
                            connection.send(str.encode('getwarehousetax'))
                            # return None when operation is sent to other node
                            return None
            return False
    for district in districtTable:
        if D_W_ID == district['D_W_ID'] and D_ID == district['D_ID']:
            D_TAX = district['D_TAX']
            D_NEXT_O_ID = district['D_NEXT_O_ID']
            district['D_NEXT_O_ID'] += 1
            break
    customerInfo = []
    for customer in customerTable:
        if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
            C_DISCOUNT = customer['C_DISCOUNT']
            C_LAST = customer['C_LAST']
            C_CREDIT = customer['C_CREDIT']
            customerInfo.append([C_DISCOUNT, C_LAST, C_CREDIT])
            break

    # ------------------------------------------------------------------------------------------------------------------
    # Adding the order to the orderTable and newOrderTable
    # ------------------------------------------------------------------------------------------------------------------

    # determines whether all of the items in I_IDS are from the same warehouse where a list of warehouse IDs respective
    #   to an item is compared to the warehouse ID: I_W_IDS = W_ID

    # also adds all item information (item price, item name, item data) to a list of items

    # O_OL_CNT is ol_cnt which is the total number of items in the order
    O_ALL_LOCAL = True
    items = []
    for i in range(len(I_IDS)):
        O_ALL_LOCAL = O_ALL_LOCAL and I_W_IDS[i] == W_ID
        for item in itemTable:
            if I_IDS[i] == item['I_ID']:
                items.append([item['I_PRICE'], item['I_NAME'], item['I_DATA']])
                badItemNumFlag = False
                break
            else:
                badItemNumFlag = True
        if badItemNumFlag == True:
            items.append([])

    O_CARRIER_ID = None
    O_OL_CNT = len(I_IDS)
    addNewOrder(newOrderTable, D_NEXT_O_ID, D_ID, W_ID)
    addOrder(orderTable, D_NEXT_O_ID, D_ID, W_ID, C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)

    # ------------------------------------------------------------------------------------------------------------------
    # Retrieve information from stockTable, update quantity and order and remote counts
    # ------------------------------------------------------------------------------------------------------------------
    itemInfo = []
    TOTAL_AMOUNT = 0
    for i in range(O_OL_CNT):
        # Fixed 1% of the New-Order Transaction are chosen at random to simulate data errors, determined based off of
        #   rbk input where if rbk is set to 1, then an invalid item value is placed into I_IDS
        if len(items[i]) == 0:
            for stock, stockI in zip(stockTable, stockTableInitial):
                stock['S_QUANTITY'] = stockI['S_QUANTITY']
                stock['S_YTD'] = stockI['S_YTD']
                stock['S_ORDER_CNT'] = stockI['S_ORDER_CNT']
                stock['S_REMOTE_CNT'] = stockI['S_REMOTE_CNT']
            return 'Item number is not valid'

        OL_NUMBER = i + 1
        OL_SUPPLY_W_ID = I_W_IDS[i]
        OL_I_ID = I_IDS[i]
        OL_QUANTITY = I_QTYS[i]
        itemInfo = items[i]
        I_PRICE = itemInfo[0]
        I_NAME = itemInfo[1]
        I_DATA = itemInfo[2]
        for stock in stockTable:
            if OL_I_ID == stock['S_I_ID'] and OL_SUPPLY_W_ID == stock['S_W_ID']:
                S_QUANTITY = stock['S_QUANTITY']
                S_YTD = stock['S_YTD']
                S_ORDER_CNT = stock['S_ORDER_CNT']
                S_REMOTE_CNT = stock['S_REMOTE_CNT']
                S_DATA = stock['S_DATA']
                S_DIST_xx = ['S_DIST_' + '{0:0=2d}'.format(D_ID)]
                break
        if S_QUANTITY >= OL_QUANTITY + 10:
            S_QUANTITY -= OL_QUANTITY
        else:
            S_QUANTITY = (S_QUANTITY - OL_QUANTITY) + 91
        S_YTD += OL_QUANTITY
        S_ORDER_CNT += 1
        if OL_SUPPLY_W_ID != W_ID:
            S_REMOTE_CNT += 1

        # Updating stock table
        for stock in stockTable:
            if OL_I_ID == stock['S_I_ID'] and OL_SUPPLY_W_ID == stock['S_W_ID']:
                stock['S_QUANTITY'] = S_QUANTITY
                stock['S_YTD'] = S_YTD
                stock['S_ORDER_CNT'] = S_ORDER_CNT
                stock['S_REMOTE_CNT'] = S_REMOTE_CNT
                break

        OL_AMOUNT = OL_QUANTITY * I_PRICE
        TOTAL_AMOUNT += OL_AMOUNT

        if I_DATA.find("ORIGINAL") != -1 and S_DATA.find("ORIGINAL") != -1:
            BRAND_GENERIC = 'B'
        else:
            BRAND_GENERIC = 'G'

        addOrderLine(orderLineTable, D_NEXT_O_ID, D_ID, W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, O_ENTRY_D, OL_QUANTITY, OL_AMOUNT,
                     S_DIST_xx)

        itemInfo.append([I_NAME, S_QUANTITY, BRAND_GENERIC, I_PRICE, OL_AMOUNT])

    TOTAL_AMOUNT *= (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX)
    otherInfo = [(W_TAX, D_TAX, D_NEXT_O_ID, TOTAL_AMOUNT)]
    return [customerInfo, otherInfo, itemInfo]


def paymentTransaction(W_ID, D_W_ID, D_ID, C_W_ID, C_D_ID, C_ID, C_LAST, H_AMOUNT, H_DATE, warehouseTable, districtTable,
                       customerTable, historyTable):
    warehouse_info = []
    district_info = []
    customer_info = []
    for warehouse in warehouseTable:
        if W_ID == warehouse['W_ID']:
            W_NAME = warehouse['W_NAME']
            W_STREET_1 = warehouse['W_STREET_1']
            W_STREET_2 = warehouse['W_STREET_2']
            W_CITY = warehouse['W_CITY']
            W_STATE = warehouse['W_STATE']
            W_ZIP = warehouse['W_ZIP']
            warehouse['W_YTD'] = warehouse['W_YTD'] + H_AMOUNT
            warehouse_info.append([W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, warehouse['W_YTD']])
            break
    for district in districtTable:
        if D_W_ID == district['D_W_ID'] and D_ID == district['D_ID']:
            D_NAME = district['D_NAME']
            D_STREET_1 = district['D_STREET_1']
            D_STREET_2 = district['D_STREET_2']
            D_CITY = district['D_CITY']
            D_STATE = district['D_STATE']
            D_ZIP = district['D_ZIP']
            district['D_YTD'] = district['D_YTD'] + H_AMOUNT
            district_info.append([D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, district['D_YTD']])
            break
    if C_ID != None:
        for customer in customerTable:
            if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
                C_FIRST = customer['C_FIRST']
                C_MIDDLE = customer['C_MIDDLE']
                C_LAST = customer['C_LAST']
                C_STREET_1 = customer['C_STREET_1']
                C_STREET_2 = customer['C_STREET_2']
                C_CITY = customer['C_CITY']
                C_STATE = customer['C_STATE']
                C_ZIP = customer['C_ZIP']
                C_PHONE = customer['C_PHONE']
                C_SINCE = customer['C_SINCE']
                C_CREDIT = customer['C_CREDIT']
                C_CREDIT_LIM = customer['C_CREDIT_LIM']
                C_DISCOUNT = customer['C_DISCOUNT']
                C_DATA = customer['C_DATA']
                customer['C_BALANCE'] = customer['C_BALANCE'] - H_AMOUNT
                customer['C_YTD_PAYMENT'] = customer['C_YTD_PAYMENT'] + H_AMOUNT
                customer['C_PAYMENT_CNT'] = customer['C_PAYMENT_CNT'] + 1
                customer_info.append([C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,
                                     C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_DATA, customer['C_BALANCE'],
                                     customer['C_YTD_PAYMENT'], customer['C_PAYMENT_CNT']])
    else:
        customer_with_lastname = []
        for customer in customerTable:
            if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_LAST == customer['C_LAST']:
                customer_with_lastname.append(customer)
        # sorts the list of customers in ascending order based off of C_FIRST
        sorted(customer_with_lastname, key=lambda i: i['C_FIRST'])
        # gets n/2 positioned customer where C_LAST is all the same
        customer_count = len(customer_with_lastname)
        index = int((customer_count-1)/2)
        picked_customer = customer_with_lastname[index]
        C_ID = picked_customer['C_ID']
        for customer in customerTable:
            if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
                C_ID = customer['C_ID']
                C_FIRST = customer['C_FIRST']
                C_MIDDLE = customer['C_MIDDLE']
                C_STREET_1 = customer['C_STREET_1']
                C_STREET_2 = customer['C_STREET_2']
                C_CITY = customer['C_CITY']
                C_STATE = customer['C_STATE']
                C_ZIP = customer['C_ZIP']
                C_PHONE = customer['C_PHONE']
                C_SINCE = customer['C_SINCE']
                C_CREDIT = customer['C_CREDIT']
                C_CREDIT_LIM = customer['C_CREDIT_LIM']
                C_DISCOUNT = customer['C_DISCOUNT']
                C_DATA = customer['C_DATA']
                customer['C_BALANCE'] = customer['C_BALANCE'] - H_AMOUNT
                customer['C_YTD_PAYMENT'] = customer['C_YTD_PAYMENT'] + H_AMOUNT
                customer['C_PAYMENT_CNT'] = customer['C_PAYMENT_CNT'] + 1
                customer_info.append([C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,
                                     C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_DATA, customer['C_BALANCE'],
                                     customer['C_YTD_PAYMENT'], customer['C_PAYMENT_CNT']])
    if C_CREDIT == 'BC':
        # map will perform the string typecasting on all of the iterables in the list, then join the strings together
        new_c_data = " ".join(map(str, [C_ID, C_D_ID, C_W_ID, D_ID, W_ID, H_AMOUNT]))
        C_DATA = (new_c_data + C_DATA)
        if len(C_DATA) > 500:
            #list slicing, will ensure that all characters to the right of 500 chars are removed
            C_DATA = C_DATA[:500]
    H_DATA = W_NAME + '    ' + D_NAME
    addToHistoryTable(historyTable, C_ID, C_D_ID, C_W_ID, D_ID, W_ID, H_DATE, H_AMOUNT, H_DATA)
    return [warehouse_info, district_info, customer_info]


def orderStatusTransaction(C_W_ID, C_D_ID, C_ID, C_LAST, customerTable, orderTable, orderLineTable):
    customer_info = []
    if C_ID != None:
        for customer in customerTable:
            if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
                C_BALANCE = customer['C_BALANCE']
                C_FIRST = customer['C_FIRST']
                C_MIDDLE = customer['C_MIDDLE']
                C_LAST = customer['C_LAST']
                customer_info.append([C_BALANCE, C_FIRST, C_MIDDLE, C_LAST])
    else:
        customer_with_lastname = []
        for customer in customerTable:
            if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_LAST == customer['C_LAST']:
                customer_with_lastname.append(customer)
        # sorts the list of customers in ascending order based off of C_FIRST
        sorted(customer_with_lastname, key=lambda i: i['C_FIRST'])
        # gets n/2 positioned customer where C_LAST is all the same
        customer_count = len(customer_with_lastname)
        index = int((customer_count-1)/2)
        picked_customer = customer_with_lastname[index]
        C_ID = picked_customer['C_ID']
        for customer in customerTable:
            if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID == customer['C_ID']:
                C_BALANCE = customer['C_BALANCE']
                C_FIRST = customer['C_FIRST']
                C_MIDDLE = customer['C_MIDDLE']
                C_LAST = customer['C_LAST']
                customer_info.append([C_BALANCE, C_FIRST, C_MIDDLE, C_LAST])
    ordertable_info = []
    O_W_ID = C_W_ID
    O_D_ID = C_D_ID
    O_C_ID = C_ID
    # TODO: Figure out what needs to be done if table is initially empty
    # for now set O_ID to None if orderTable is empty
    O_ID = None
    for order in orderTable:
        if O_W_ID == order['O_W_ID'] and O_D_ID == order['O_D_ID'] and O_C_ID == order['O_C_ID']:
            O_ID = order['O_ID']
            O_ENTRY_D = order['O_ENTRY_D']
            O_CARRIER_ID = order['O_CARRIER_ID']
            ordertable_info.append([O_ID, O_ENTRY_D, O_CARRIER_ID])
    orderlinetable_info = []
    OL_W_ID = O_W_ID
    OL_D_ID = O_D_ID
    OL_O_ID = O_ID
    for orderLine in orderLineTable:
        if OL_W_ID == orderLine['OL_W_ID'] and OL_D_ID == orderLine['OL_W_ID'] and OL_O_ID == orderLine['OL_O_ID']:
            OL_I_ID = orderLine['OL_I_ID']
            OL_SUPPLY_W_ID = orderLine['OL_SUPPLY_W_ID']
            OL_QUANTITY = orderLine['OL_QUANTITY']
            OL_AMOUNT = orderLine['OL_AMOUNT']
            OL_DELIVERY_D = orderLine['OL_DELIVERY_D']
            orderlinetable_info.append([OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D])
    return [customer_info, ordertable_info, orderlinetable_info]


def deliveryTransaction(W_ID, O_CARRIER_ID, OL_DELIVERY_D, customerTable, newOrderTable, orderTable, orderLineTable):
    result_file = []
    for D_ID in range(1, 11):
        NO_W_ID = W_ID
        NO_D_ID = D_ID
        low_NO_O_ID = float('inf')
        for newOrder in newOrderTable:
            if NO_W_ID == newOrder['NO_W_ID'] and NO_D_ID == newOrder['NO_D_ID'] and newOrder['NO_O_ID'] < low_NO_O_ID:
                low_NO_O_ID = newOrder['NO_O_ID']
        if low_NO_O_ID == float('inf'):
            #TODO: if transaction is skipped, and skipped amount of transactions is more than 1% of total transactions,
            # then must report this. The ending result file produced should show percentage of skipped deliveries and
            # skipped districts
            continue
        deleteNewOrder(newOrderTable, low_NO_O_ID, NO_D_ID, NO_W_ID)
        O_W_ID = W_ID
        O_D_ID = D_ID
        O_ID = low_NO_O_ID
        for order in orderTable:
            if O_W_ID == order['O_W_ID'] and O_D_ID == order['O_D_ID'] and O_ID == order['O_ID']:
                O_C_ID = order['O_C_ID']
                order['O_CARRIER_ID'] = O_CARRIER_ID
        OL_W_ID = O_W_ID
        OL_D_ID = O_D_ID
        OL_O_ID = O_ID
        sum_total = 0
        for orderLine in orderLineTable:
            if OL_W_ID == orderLine['OL_W_ID'] and OL_D_ID == orderLine['OL_D_ID'] and OL_O_ID == orderLine['OL_O_ID']:
                orderLine['OL_DELIVERY_D'] = OL_DELIVERY_D
                sum_total += orderLine['OL_AMOUNT']
        C_W_ID = W_ID
        C_D_ID = D_ID
        C_ID = O_C_ID
        for customer in customerTable:
            if C_W_ID == customer['C_W_ID'] and C_D_ID == customer['C_D_ID'] and C_ID  == customer['C_ID']:
                customer['C_BALANCE'] += sum_total
                customer['C_DELIVERY_CNT'] += 1
        result_file.append((D_ID, low_NO_O_ID))
    return result_file


def stockLevelTransaction(W_ID, D_ID, threshold, districtTable, orderLineTable, stockTable):
    low_stock = 0
    D_W_ID = W_ID
    for district in districtTable:
        if D_W_ID == district['D_W_ID'] and D_ID == district['D_ID']:
            D_NEXT_O_ID = district['D_NEXT_O_ID']
    OL_W_ID = W_ID
    OL_D_ID = D_ID
    for orderLine in orderLineTable:
        if OL_W_ID == orderLine['OL_W_ID'] and OL_D_ID == orderLine['OL_D_ID'] and \
                (orderLine['OL_O_ID'] < D_NEXT_O_ID or orderLine['OL_O_ID'] >= (D_NEXT_O_ID - 20)):
            S_I_ID = orderLine['OL_I_ID']
            S_W_ID = W_ID
            for stock in stockTable:
                if S_I_ID == stock['S_I_ID'] and S_W_ID == stock['S_W_ID'] and stock['S_QUANTITY'] < threshold:
                    low_stock += 1
    return low_stock


'''
# Number of Warehouses
numOfWarehouses = 1

# Create Tables
warehouseTable = createWarehouseTable(numOfWarehouses)
districtTable = createDistrictTable(numOfWarehouses)
customerTable = createCustomerTable(numOfWarehouses)
historyTable = createHistoryTable()
newOrderTable = createNewOrderTable()
orderTable = createOrderTable()
orderLineTable = createOrderLineTable()
itemTable = createItemTable()
stockTable = createStockTable(numOfWarehouses)


# Create input parameters for newOrderTransaction
W_ID = 1
D_ID = random.randint(1, 10)
C_ID = NURand(1023, 1, 3000)
ol_cnt = random.randint(5, 15)
rbk = random.randint(1, 100)
I_IDS = []
I_W_IDS = []
I_QTYS = []
for item in range(ol_cnt):
    if item == ol_cnt-1 and rbk == 1:
        OL_I_ID = 3000000
    else:
        OL_I_ID = NURand(8191, 1, 100000)
    I_IDS.append(OL_I_ID)
    x = random.randint(1, 100)
    if x > 1:
        OL_SUPPLY_W_ID = W_ID
    if x == 1:
        list_W_IDS = []
        for i in range(1, numOfWarehouses+1):
            list_W_IDS.append(i)
        list_W_IDS.remove(W_ID)
        OL_SUPPLY_W_ID = list_W_IDS[random.randint(0, (len(list_W_IDS)-1))]
    I_W_IDS.append(OL_SUPPLY_W_ID)
    OL_QUANTITY = random.randint(1, 10)
    I_QTYS.append(OL_QUANTITY)
O_ENTRY_D = date.today().strftime("%d/%m/%Y")

# W_ID, D_W_ID, D_ID, C_W_ID, C_D_ID, C_ID, O_ENTRY_D, I_IDS, I_W_IDS, I_QTYS

start = time.process_time()
result = newOrderTransaction(W_ID, W_ID, D_ID, W_ID, D_ID, C_ID, O_ENTRY_D, I_IDS, I_W_IDS, I_QTYS,
                    warehouseTable, districtTable, customerTable, itemTable, newOrderTable, orderLineTable, orderTable, stockTable)
end = time.process_time()
print(result)
print(str(end-start) + ' sec')


# Create input parameters for paymentTransaction
W_ID = 1
D_W_ID = W_ID
D_ID = random.randint(1, 10)
x = random.randint(1, 100)
y = random.randint(1, 100)
if x <= 85:
    C_D_ID = D_ID
    C_W_ID = W_ID
else:
    C_D_ID = random.randint(1, 10)
    C_W_ID = random.randint(1, numOfWarehouses)
if y <= 60:
    lastNameList = ['Robinson', 'Nguyen', 'Juarez']
    C_LAST = lastNameList[NURand(255, 0, 999) % len(lastNameList)]
    C_ID = None
else:
    C_LAST = None
    C_ID = NURand(1023, 1, 3000)
print(C_LAST)
H_AMOUNT = round(random.uniform(1, 5000000), 2)
H_DATE = date.today().strftime("%d/%m/%Y")
start = time.process_time()
result = paymentTransaction(W_ID, D_W_ID, D_ID, C_W_ID, C_D_ID, C_ID, C_LAST, H_AMOUNT, H_DATE, warehouseTable, districtTable,
                   customerTable, historyTable)
end = time.process_time()
print(result)
print(str(end - start) + ' sec')


# Create input parameters for orderStatusTransaction
W_ID = 1
D_ID = random.randint(1, 10)
y = random.randint(1, 100)
if y <= 60:
    lastNameList = ['Robinson', 'Nguyen', 'Juarez']
    C_LAST = lastNameList[NURand(255, 0, 999) % len(lastNameList)]
    C_ID = None
else:
    C_LAST = None
    C_ID = NURand(1023, 1, 3000)
start = time.process_time()
result = orderStatusTransaction(W_ID, D_ID, C_ID, C_LAST, customerTable, orderTable, orderLineTable)
end = time.process_time()
print(result)
print(str(end-start) + ' sec')


# Create input parameters for deliveryTransaction
W_ID = 1
O_CARRIER_ID = random.randint(1, 10)
OL_DELIVERY_D = date.today().strftime("%d/%m/%Y")
start = time.process_time()
result = deliveryTransaction(W_ID, O_CARRIER_ID, OL_DELIVERY_D, customerTable, newOrderTable, orderTable, orderLineTable)
end = time.process_time()
print(result)
print(str(end-start) + ' sec')


# Create input parameters for stockLevelTransaction
W_ID = 1
D_ID = 1
threshold = random.randint(10, 20)
start = time.process_time()
result = stockLevelTransaction(W_ID, D_ID, threshold, districtTable, orderLineTable, stockTable)
end = time.process_time()
print(result)
print(str(end-start) + ' sec')
'''
