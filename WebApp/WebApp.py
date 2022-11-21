from aiohttp import web
from aiohttp.web_app import Application
import asyncio
import asyncpg

DB_USER = "postgres"
DB_NAME = "postgres"
DB_PASSWORD = "tchu"
DB_HOST = "127.0.0.1"

routes = web.RouteTableDef() # задаём возможность обслуживать запросы по разным путям

#обрабатывает GET-запрос на получение всех товарных позиций
@routes.get('/getAllItems/')
async def getAllItems(request):
    pool = request.app['pool']
    async with pool.acquire() as connection:
        async with connection.transaction():
            result = await connection.fetch('SELECT * FROM item')
            result_as_dict: List[Dict] = [dict(item) for item in result]
            return web.json_response(result_as_dict)

#обрабатывает GET-запрос на получение всех магазинов
@routes.get('/getAllStores/')
async def getAllStores(request):
    return web.Response(text="Hello, stores")

#обрабатывает POST-запрос с json-телом для сохранения данных о произведенной продаже (id товара + id магазина)
@routes.post('/postSale/')
async def postSale(request):
    return web.Response(text="Hello, sale")

#обрабатывает GET-запрос на получение данных по топ 10 самых доходных магазинов за месяц (id + адреса + суммарная выручка)
@routes.get('/getTopStoresForMonth/')
async def getTopStoresForMonth(request):
    return web.Response(text="Hello, TopStoresForMonth")

#обрабатывает GET-запрос на получение данных по топ 10 самых продаваемых товаров (id + наименование + количество проданных товаров)
@routes.get('/getTenBestSellItems/')
async def getTenBestSellItems(request):
    return web.Response(text="Hello, TenBestSellItems")

async def create_database_pool(app: Application):
    app['pool'] = await asyncpg.create_pool(user=DB_USER, password=DB_PASSWORD,
                                 database=DB_NAME, host=DB_HOST)

async def destroy_database_pool(app: Application):
    """Уничтожение пула подключений к бд."""
    await  app['pool'].close()

app = web.Application()
app.on_startup.append(create_database_pool)
app.on_cleanup.append(destroy_database_pool)
app.add_routes(routes)
web.run_app(app)