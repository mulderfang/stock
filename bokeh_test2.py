from bokeh.plotting import figure,show,output_file
x_values = [1, 2, 3, 4, 5]
y_values = [6, 7, 2, 3, 6]
output_file('test.html')
p = figure()
p.line(x_values,y_values)
show(p)