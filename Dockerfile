FROM ruby:2.6

# throw errors if Gemfile has been modified since Gemfile.lock
# RUN bundle config --global frozen 1

WORKDIR /opt/app

RUN git clone https://github.com/bill538/billsnet_solar.git /opt/app

RUN gem update 
RUN gem install 

CMD ["/bin/bash","/opt/app/bin/influx_load_solar.rb","/opt/app/bin/read_smatool_live.rb"]
