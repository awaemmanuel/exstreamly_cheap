from app import app

@app.route('/')
@app.route('/index')
def index():
    return '<h1 style="font-size:50px;text-align:center;color:red">ExStreamly Cheap is still under construction</h1>'

