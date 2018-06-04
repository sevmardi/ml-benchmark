import pandas as pd
# https://github.com/lyokka/Classification-with-Vowpal-Wabbit/blob/master/notebook.ipynb
#load oaa_loss.csv
oaa_loss = pd.read_csv('data/oaa_loss.csv')
oaa_loss = oaa_loss.round(4) # keep 4 digits
oaa_train_loss = oaa_loss[oaa_loss['train/test']==1] # split to test loss and train loss
oaa_test_loss = oaa_loss[oaa_loss['train/test']==0]

ect_loss = pd.read_csv('data/ect_loss.csv')
ect_loss = ect_loss.round(4)
ect_train_loss = ect_loss[ect_loss['train/test']==1]
ect_test_loss = ect_loss[ect_loss['train/test']==0]


from bokeh.io import output_notebook, show
from bokeh.models import LinearColorMapper, ColorBar, ColumnDataSource, LabelSet
from bokeh.palettes import Magma256, Viridis256
from bokeh.plotting import figure
from bokeh.layouts import gridplot
from bokeh.models import HoverTool
# output_notebook()

# prepare data
source_ect = ColumnDataSource(ect_test_loss)
source_oaa = ColumnDataSource(oaa_test_loss)

# define color map
color_mapper1 = LinearColorMapper(palette=Viridis256,
                                  low = 0,
                                 #low=ect_test_loss.average_loss.min(),
                                 high = 0.21)
                                 #high=ect_test_loss.average_loss.max())
color_mapper2 = LinearColorMapper(palette=Viridis256,
                                  low = 0,
                                 #low=oaa_test_loss.average_loss.min(),
                                  high = 0.21)
                                 #high=oaa_test_loss.average_loss.max())

# define color bar
color_bar1 = ColorBar(color_mapper=color_mapper1, label_standoff=12, location=(0,0), title='average_loss')
color_bar2 = ColorBar(color_mapper=color_mapper2, label_standoff=12, location=(0,0), title='average_loss')

# define hover tool
hover1 = HoverTool(tooltips=[("Method", "ect"),
                            ("passes", "@passes"),
                            ("learning_rate", "@learning_rate"),
                            ("average_loss", "@average_loss"),])
hover2 = HoverTool(tooltips=[("Method", "oaa"),
                            ("passes", "@passes"),
                            ("learning_rate", "@learning_rate"),
                            ("average_loss", "@average_loss"),])

# define figure
s1 = figure(plot_width = 400,
            plot_height= 400,
            x_axis_type='log',
            tools=[hover1],
            title = 'ect Test Loss')
# define scatter plot
s1.circle(x = 'learning_rate',
          y = 'passes',
          fill_color={'field': 'average_loss', 'transform': color_mapper1},
          line_color='black',
          size = 10,
          source = source_ect)
#s1.add_layout(color_bar1, 'left')

# define second figure
s2 = figure(plot_width = 400,
            plot_height= 400,
            x_axis_type='log',
            tools=[hover2],
            title = 'oaa Test Loss')
# define second scatter plot
s2.circle(x = 'learning_rate',
          y = 'passes',
          size = 10,
          fill_color={'field': 'average_loss', 'transform': color_mapper2},
          line_color='black',
          source = source_oaa)

# add color bar
s2.add_layout(color_bar2, 'right')

# two plot on same row
p = gridplot([[s1, s2]])

# display
show(p)


print('minimum ect test average loss')
ect_test_loss[ect_test_loss.average_loss == ect_test_loss.average_loss.min()]


print('minimum ect test average loss')
oaa_test_loss[oaa_test_loss.average_loss == oaa_test_loss.average_loss.min()]
