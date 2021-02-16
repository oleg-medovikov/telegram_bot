from zencad import *

r = 40          # радиус без зуба
r_zub = 50      # радиус вместе с зубом
N_zub = 20      # количество зубов
x_zub = 2       # толщина конца зуба


def gear_profile(r, r_zub, N_zub, x_zub, n_tochek=20):
    """
    r:         радиус без зуба
    r_zub:     радиус вместе с зубом
    N_zub:     количество зубов
    x_zub:     толщина конца зуба
    n_tochek:  количество точек в апроксимации эвольвенты
    """

    # Максимальная длинна эвольвенты
    alfa = math.sqrt(r_zub**2 - r**2)/r
    # Угол дуги окружности, которую занимает эвольвента
    omega = math.asin((r*(math.sin(alfa) - alfa*math.cos(alfa)) )/ r_zub)
    # Угол дуги окружности, которую занимает зуб
    teta = 2*math.pi/N_zub
    # Угол смещения второй грани зуба относительно первой
    ax = 2*math.asin(x_zub/(2*r_zub))
    d =  - 2 * omega - ax
    
    #pnts=[]
    
    wires = []
    abases = []
    bbases = []
    
    for k in range(N_zub):
        apnts = []
        for j in range(0,n_tochek+1):
            a = j*alfa/n_tochek
            # Считаем точки эвольвенты первой грани
            x = r*(math.cos(a) + a*math.sin(a))
            y = r*(math.sin(a) - a*math.cos(a))
            # Добавляем точки первой эвольвенты относительно повернутой системы координат
            apnts.append([x*math.cos(-k*teta) + y*math.sin(-k*teta),-x*math.sin(-k*teta)+y*math.cos(-k*teta)])
        bpnts = []
        for j in range(n_tochek,0,-1):
            a = j*alfa/n_tochek
            # Считаем точки эвольвенты второй грани
            x = r*(math.cos(a) + a*math.sin(a))
            y = -r*(math.sin(a) - a*math.cos(a))
            # Добавляем точки второй эвольвенты относительно повернутой системы координат
            bpnts.append([x*math.cos(-k*teta +d) + y*math.sin(-k*teta+d),-x*math.sin(-k*teta+d)+y*math.cos(-k*teta+d)])
        # Создать эвольвентные рёбра зуба
        wires.append(interpolate(apnts))
        wires.append(interpolate(bpnts))
        
        # Создать рёбро вершины зуба
        wires.append(segment(apnts[-1], bpnts[0]))
        
        # Запомнить точки основания зубьев
        abases.append(apnts[0])
        bbases.append(bpnts[-1])
    # Добавляем рёбра между основанием зубьев 
    wires.append(segment(abases[0], bbases[-1]))
    for k in range(N_zub - 1):
            wires.append(segment(abases[k+1], bbases[k]))
    
    # Собираем все элементы в единый wire
    evolv = sew(wires)
    
    return evolv.fill()

def create_big_gear():
    m = gear_profile(20, 25, 18, 1)
    o = nullshape()
    for i in range(3):
        o += cylinder(14,5,True).moveX(18).rotateZ(deg(i*120 + 6))
    o = o ^ cylinder(18,5,True)
    gear = linear_extrude(proto=m, vec=(0,0,4), center=True) \
            - o + cylinder(4,4,True) - cylinder(2,4,True)
    return gear

disp(create_big_gear())
show()
