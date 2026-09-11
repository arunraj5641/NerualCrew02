# Stage 1: Build React/Vite application
FROM node:20-alpine AS builder

WORKDIR /app
COPY package*.json ./
RUN npm ci || npm install
COPY . .
RUN npm run build

# Stage 2: Production non-root nginx image
FROM nginx:1.27-alpine

# Non-root user setup per handout requirement #9
# 1. Allow nginx to bind port 80 as non-root using libcap
# 2. Configure pid path in /tmp and fix directory permissions
RUN apk add --no-cache libcap && \
    setcap 'cap_net_bind_service=+ep' /usr/sbin/nginx && \
    sed -i '/user  nginx;/d' /etc/nginx/nginx.conf && \
    sed -i 's|/run/nginx.pid|/tmp/nginx.pid|g' /etc/nginx/nginx.conf && \
    touch /tmp/nginx.pid && \
    chown -R nginx:nginx /tmp/nginx.pid /var/cache/nginx /var/log/nginx /etc/nginx/conf.d

# Copy compiled React production bundle from builder
COPY --from=builder /app/dist /usr/share/nginx/html
RUN chown -R nginx:nginx /usr/share/nginx/html

USER nginx
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
