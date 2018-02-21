def myfunc(anotherfunc, *extraArgs):
      return anotherfunc(*extraArgs)
def pd_apply(df,func,*extraArgs):
    return  df.rdd.map(
                    lambda line:
                    myfunc(func,line,*extraArgs)
                    ).toDF()
