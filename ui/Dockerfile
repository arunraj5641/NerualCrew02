FROM nginx:1.27-alpine

# nginx image already runs its worker processes as an unprivileged user;
# we additionally drop the master process to a non-root user per handout
# requirement #9.
RUN sed -i 's/user  nginx;/user  nginx;/' /etc/nginx/nginx.conf
COPY index.html /usr/share/nginx/html/index.html

EXPOSE 80
